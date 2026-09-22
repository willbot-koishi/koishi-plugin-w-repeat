import { MongoClient } from 'mongodb'

interface Candidate {
  id: number
  senders: string[]
  endTime?: number
  suspensions?: Array<{ suspendTime: number, resumeTime: number }>
}

interface CandidateGroup {
  count: number
  docs: Candidate[]
}

interface CleanupEntry {
  keepId: number
  deleteIds: number[]
  suspensions: Array<{ suspendTime: number, resumeTime: number }>
}

const BATCH_SIZE = 500
const apply = process.argv.includes('--apply')
const resumeIndex = process.argv.indexOf('--resume')
const resumeSuffix = resumeIndex >= 0 ? process.argv[resumeIndex + 1] : undefined
const uri = process.env.MONGO_URI

if (! uri) throw new Error('MONGO_URI is required')

const isPrefix = (prefix: string[], value: string[]) =>
  prefix.length <= value.length && prefix.every((item, index) => item === value[index])

const senderKey = (senders: string[]) => JSON.stringify(senders)

const uniqueSuspensions = (docs: Candidate[]) => [ ...new Map(docs
  .flatMap(doc => doc.suspensions ?? [])
  .map(item => [`${item.suspendTime}:${item.resumeTime}`, item] as const)
).values() ].sort((a, b) => a.suspendTime - b.suspendTime)

const chunks = function* <T>(values: T[], size: number) {
  for (let index = 0; index < values.length; index += size) {
    yield values.slice(index, index + size)
  }
}

async function main() {
  const client = new MongoClient(uri)
  await client.connect()
  try {
    const db = client.db()
    const records = db.collection('w-repeat-record')

    const applyCleanup = async (
      plan: CleanupEntry[],
      backupName: string,
      planName: string,
      prepared: boolean
    ) => {
      const deleteIds = plan.flatMap(entry => entry.deleteIds)
      const affectedIds = plan.flatMap(entry => [ entry.keepId, ...entry.deleteIds ])
      const backup = db.collection(backupName)
      const savedPlan = db.collection(planName)

      if (! prepared) {
        await savedPlan.insertMany(plan.map((entry, index) => ({ index, ...entry })))
        for (const ids of chunks(affectedIds, BATCH_SIZE)) {
          await records.aggregate([
            { $match: { id: { $in: ids } } },
            { $merge: { into: backupName, whenMatched: 'keepExisting', whenNotMatched: 'insert' } },
          ]).toArray()
        }
      }

      const [ backupCount, savedPlanCount ] = await Promise.all([
        backup.countDocuments(),
        savedPlan.countDocuments(),
      ])
      const uniqueAffectedCount = new Set(affectedIds).size
      if (backupCount !== uniqueAffectedCount || savedPlanCount !== plan.length) {
        throw new Error(
          `backup verification failed: expected ${uniqueAffectedCount}/${plan.length}, ` +
          `got ${backupCount}/${savedPlanCount}`
        )
      }

      // Most canonical records already contain the complete suspension list.
      // Project only metadata and avoid rewriting large base64-bearing documents.
      const currentSuspensions = new Map<number, string>()
      for (const ids of chunks(plan.map(entry => entry.keepId), BATCH_SIZE)) {
        const docs = await records
          .find({ id: { $in: ids } }, { projection: { _id: 0, id: 1, suspensions: 1 } })
          .toArray()
        docs.forEach(doc => currentSuspensions.set(doc.id, JSON.stringify(doc.suspensions ?? [])))
      }
      const suspensionUpdates = plan.filter(entry =>
        currentSuspensions.get(entry.keepId) !== JSON.stringify(entry.suspensions)
      )
      for (const entries of chunks(suspensionUpdates, BATCH_SIZE)) {
        await records.bulkWrite(entries.map(entry => ({
          updateOne: {
            filter: { id: entry.keepId },
            update: { $set: { suspensions: entry.suspensions } },
          }
        })), { ordered: false })
      }
      for (const ids of chunks(deleteIds, BATCH_SIZE)) {
        await records.deleteMany({ id: { $in: ids } })
      }

      const remainingDeletedIds = await records.countDocuments({ id: { $in: deleteIds } })
      if (remainingDeletedIds) {
        throw new Error(`${remainingDeletedIds} planned duplicate records remain after cleanup`)
      }

      console.log(JSON.stringify({
        applied: true,
        deleted: deleteIds.length,
        suspensionUpdates: suspensionUpdates.length,
        backupCollection: backupName,
        planCollection: planName,
      }))
    }

    if (resumeSuffix) {
      if (! apply) throw new Error('--resume requires --apply')
      const backupName = `w-repeat-record-duplicate-backup-${resumeSuffix}`
      const planName = `w-repeat-record-duplicate-plan-${resumeSuffix}`
      const savedPlan = await db.collection<CleanupEntry>(planName)
        .find({}, { projection: { _id: 0, keepId: 1, deleteIds: 1, suspensions: 1 } })
        .toArray()
      await applyCleanup(savedPlan, backupName, planName, true)
      return
    }

    // Full base64 values are intentionally not used as aggregation keys: doing
    // so can exceed MongoDB's 16 MiB document limit. Size plus both ends is a
    // strong identity within the same guild, timestamp and serialized content.
    const imageSignature = {
      $map: {
        input: { $ifNull: [ '$images', [] ] },
        as: 'image',
        in: {
          $cond: [
            { $eq: [ '$$image', null ] },
            null,
            {
              $let: {
                vars: { b64: { $ifNull: [ '$$image.b64', '' ] } },
                in: {
                  size: { $strLenBytes: '$$b64' },
                  head: { $substrBytes: [ '$$b64', 0, 96 ] },
                  tail: {
                    $substrBytes: [
                      '$$b64',
                      { $max: [ 0, { $subtract: [ { $strLenBytes: '$$b64' }, 96 ] } ] },
                      96,
                    ]
                  },
                }
              }
            }
          ]
        }
      }
    }

    const cursor = records.aggregate<CandidateGroup>([
      {
        $project: {
          gid: 1,
          startTime: 1,
          content: 1,
          imageSignature,
          candidate: {
            id: '$id',
            senders: { $ifNull: [ '$senders', [] ] },
            endTime: '$endTime',
            suspensions: { $ifNull: [ '$suspensions', [] ] },
          }
        }
      },
      {
        $group: {
          _id: {
            gid: '$gid',
            startTime: '$startTime',
            content: '$content',
            imageSignature: '$imageSignature',
          },
          count: { $sum: 1 },
          docs: { $push: '$candidate' },
        }
      },
      { $match: { count: { $gt: 1 } } },
      { $project: { _id: 0, count: 1, docs: 1 } },
    ], { allowDiskUse: true })

    const plan: CleanupEntry[] = []
    let branchedGroups = 0
    let exactBranchedGroups = 0
    let candidateGroups = 0
    let candidateRows = 0

    for await (const group of cursor) {
      candidateGroups ++
      candidateRows += group.count

      const uniqueSenders = [ ...new Map(group.docs
        .map(doc => [senderKey(doc.senders), doc.senders] as const)
      ).values() ].sort((a, b) => a.length - b.length)
      const isChain = uniqueSenders.every((senders, index) =>
        index === 0 || isPrefix(uniqueSenders[index - 1], senders)
      )
      if (! isChain) {
        branchedGroups ++
        const branches = new Map<string, Candidate[]>()
        for (const doc of group.docs) {
          const key = senderKey(doc.senders)
          const docs = branches.get(key) ?? []
          docs.push(doc)
          branches.set(key, docs)
        }
        for (const docs of branches.values()) {
          if (docs.length < 2) continue
          exactBranchedGroups ++
          const ranked = [ ...docs ].sort((a, b) =>
            (b.suspensions?.length ?? 0) - (a.suspensions?.length ?? 0) ||
            (b.endTime ?? 0) - (a.endTime ?? 0) ||
            b.id - a.id
          )
          plan.push({
            keepId: ranked[0].id,
            deleteIds: ranked.slice(1).map(doc => doc.id),
            suspensions: uniqueSuspensions(docs),
          })
        }
        continue
      }

      const ranked = [ ...group.docs ].sort((a, b) =>
        b.senders.length - a.senders.length ||
        (b.suspensions?.length ?? 0) - (a.suspensions?.length ?? 0) ||
        (b.endTime ?? 0) - (a.endTime ?? 0) ||
        b.id - a.id
      )
      plan.push({
        keepId: ranked[0].id,
        deleteIds: ranked.slice(1).map(doc => doc.id),
        suspensions: uniqueSuspensions(group.docs),
      })
    }

    const deleteIds = plan.flatMap(entry => entry.deleteIds)
    console.log(JSON.stringify({
      mode: apply ? 'apply' : 'dry-run',
      candidateGroups,
      candidateRows,
      cleanableGroups: plan.length,
      deleteCount: deleteIds.length,
      branchedGroups,
      exactBranchedGroups,
    }))
    if (! apply) return

    const suffix = new Date().toISOString().replace(/[-:.]/g, '')
    const backupName = `w-repeat-record-duplicate-backup-${suffix}`
    const planName = `w-repeat-record-duplicate-plan-${suffix}`
    await applyCleanup(plan, backupName, planName, false)
  }
  finally {
    await client.close()
  }
}

void main().catch(error => {
  console.error(error)
  process.exitCode = 1
})
