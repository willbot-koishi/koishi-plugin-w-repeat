import { MongoClient } from 'mongodb'

interface RepeatUser {
  uid: string
  repeatTime: number
  repeatCount: number
  beRepeatedTime: number
  beRepeatedCount: number
  interruptTime: number
}

const uri = process.env.MONGO_URI
if (! uri) throw new Error('MONGO_URI is required')

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
    const users = new Map<string, RepeatUser>()
    const getUser = (uid: string) => {
      let user = users.get(uid)
      if (! user) {
        user = {
          uid,
          repeatTime: 0,
          repeatCount: 0,
          beRepeatedTime: 0,
          beRepeatedCount: 0,
          interruptTime: 0,
        }
        users.set(uid, user)
      }
      return user
    }

    const cursor = records.find({}, {
      projection: { _id: 0, senders: 1, interrupter: 1 }
    })
    for await (const { senders = [], interrupter } of cursor) {
      if (! senders.length) continue
      const starter = getUser(senders[0])
      starter.beRepeatedTime ++
      starter.beRepeatedCount += senders.length - 1

      const counted = new Set<string>()
      for (const uid of senders.slice(1)) {
        const user = getUser(uid)
        user.repeatCount ++
        if (! counted.has(uid)) {
          counted.add(uid)
          user.repeatTime ++
        }
      }
      if (interrupter) getUser(interrupter).interruptTime ++
    }

    const wordRows = await records.aggregate<{
      gid: string
      word: string
      tag: string
      count: number
    }>([
      { $unwind: '$words' },
      {
        $group: {
          _id: { gid: '$gid', word: '$words.word', tag: '$words.tag' },
          count: { $sum: 1 },
        }
      },
      {
        $project: {
          _id: 0,
          gid: '$_id.gid',
          word: '$_id.word',
          tag: '$_id.tag',
          count: 1,
        }
      },
    ], { allowDiskUse: true }).toArray()

    const suffix = new Date().toISOString().replace(/[-:.]/g, '')
    const sourceNames = [ 'w-repeat-user', 'w-repeat-calendar', 'w-repeat-word' ]
    const backupNames = Object.fromEntries(sourceNames.map(name => [
      name,
      `${name}-duplicate-cleanup-backup-${suffix}`,
    ]))

    for (const sourceName of sourceNames) {
      await db.collection(sourceName).aggregate([
        { $match: {} },
        {
          $merge: {
            into: backupNames[sourceName],
            whenMatched: 'keepExisting',
            whenNotMatched: 'insert',
          }
        },
      ]).toArray()
      const [ sourceCount, backupCount ] = await Promise.all([
        db.collection(sourceName).countDocuments(),
        db.collection(backupNames[sourceName]).countDocuments(),
      ])
      if (sourceCount !== backupCount) {
        throw new Error(`backup verification failed for ${sourceName}: ${sourceCount} != ${backupCount}`)
      }
    }

    const userRows = [ ...users.values() ]
    for (const batch of chunks(userRows, 500)) {
      await db.collection('w-repeat-user').bulkWrite(batch.map(user => ({
        updateOne: {
          filter: { uid: user.uid },
          update: { $set: user },
          upsert: true,
        }
      })), { ordered: false })
    }
    await db.collection('w-repeat-user').deleteMany({
      uid: { $nin: userRows.map(user => user.uid) }
    })

    await db.collection('w-repeat-word').deleteMany({})
    for (const batch of chunks(wordRows, 1000)) {
      await db.collection('w-repeat-word').insertMany(batch, { ordered: false })
    }

    // Calendar rows are a cache and will be regenerated lazily by commands.
    await db.collection('w-repeat-calendar').deleteMany({})

    const result = {
      users: await db.collection('w-repeat-user').countDocuments(),
      words: await db.collection('w-repeat-word').countDocuments(),
      calendarRows: await db.collection('w-repeat-calendar').countDocuments(),
      backupCollections: backupNames,
    }
    if (result.users !== userRows.length || result.words !== wordRows.length || result.calendarRows) {
      throw new Error(`derived data verification failed: ${JSON.stringify(result)}`)
    }
    console.log(JSON.stringify(result))
  }
  finally {
    await client.close()
  }
}

void main().catch(error => {
  console.error(error)
  process.exitCode = 1
})
