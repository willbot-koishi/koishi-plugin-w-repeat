import { $, h, type Context } from 'koishi'
import type { SavedMessage } from 'koishi-plugin-w-message-db'

import type {
  RepeatMigration,
  RepeatMigrationStatus,
  RepeatRecordV2,
  RepeatSuspensionV2,
} from './model-v2'

export const REPEAT_MESSAGE_MIGRATION_ID = 'message-db-v2'
const MATCH_TOLERANCE = 5000

interface LegacyRepeatRecord {
  id: number
  gid: string
  content?: string
  senders: string[]
  startTime: number
  endTime: number
  interrupter?: string
  suspensions?: Array<{ suspendTime: number, resumeTime: number }>
}

export type RepeatMatchResult =
  | {
    status: 'migrated'
    messages: SavedMessage[]
    interrupter?: SavedMessage
    suspensions: RepeatSuspensionV2[]
  }
  | { status: 'unmatched' | 'ambiguous', reason: string }

export interface RepeatMigrationProgress {
  state: RepeatMigration
  current?: { id: number, status: RepeatMigrationStatus, reason: string }
}

export interface RepeatMigrationOptions {
  batchSize?: number
  signal?: AbortSignal
  onProgress?: (progress: RepeatMigrationProgress) => void
}

const normalizeContent = (content = '') => h.transform(
  content.replace(/@__KOISHI_IMG__@/g, '<img/>'),
  {
    img: () => '[asset:image]',
    image: () => '[asset:image]',
    audio: () => '[asset:audio]',
    video: () => '[asset:video]',
    file: () => '[asset:file]',
  },
)

const getUid = (message: SavedMessage) => `${message.platform}:${message.userId}`

interface SequenceMatch {
  count: 0 | 1 | 2
  messages?: SavedMessage[]
}

function findRepeatSequence(record: LegacyRepeatRecord, candidates: SavedMessage[]): SequenceMatch {
  const normalized = normalizeContent(record.content)
  // states[position] describes the ways to match the first `position` senders.
  // Counts are capped at two because callers only distinguish none, unique, and ambiguous.
  const states: SequenceMatch[] = Array.from(
    { length: record.senders.length + 1 },
    () => ({ count: 0 }),
  )
  states[0] = { count: 1, messages: [] }

  for (const message of candidates) {
    if (normalizeContent(message.content) !== normalized) continue
    const uid = getUid(message)

    // Walk backwards so one candidate cannot fill multiple sender positions.
    for (let position = record.senders.length - 1; position >= 0; position --) {
      if (record.senders[position] !== uid) continue
      if (position === 0 && Math.abs(message.timestamp - record.startTime) > MATCH_TOLERANCE) continue

      const source = states[position]
      if (! source.count) continue
      const target = states[position + 1]
      const count = Math.min(target.count + source.count, 2) as SequenceMatch['count']
      states[position + 1] = count === 1
        ? { count, messages: [...source.messages!, message] }
        : { count }
    }
  }

  return states.at(-1)!
}

function findNearest(
  candidates: SavedMessage[],
  timestamp: number,
  description: string,
): SavedMessage | RepeatMatchResult {
  const ranked = candidates
    .map(message => ({ message, distance: Math.abs(message.timestamp - timestamp) }))
    .filter(item => item.distance <= MATCH_TOLERANCE)
    .sort((a, b) => a.distance - b.distance || a.message.key.localeCompare(b.message.key))
  if (! ranked.length) return { status: 'unmatched', reason: `missing ${description}` }
  if (ranked[1]?.distance === ranked[0].distance) {
    return { status: 'ambiguous', reason: `ambiguous ${description}` }
  }
  return ranked[0].message
}

export function matchLegacyRepeat(
  record: LegacyRepeatRecord,
  candidates: SavedMessage[],
): RepeatMatchResult {
  const sequence = findRepeatSequence(record, candidates)
  if (! sequence.count) return { status: 'unmatched', reason: 'repeat message sequence not found' }
  if (sequence.count > 1) return { status: 'ambiguous', reason: 'multiple repeat message sequences' }
  const messages = sequence.messages!
  const lastTimestamp = messages.at(-1).timestamp

  let interrupter: SavedMessage | undefined
  if (record.interrupter) {
    const result = findNearest(
      candidates.filter(message =>
        message.timestamp >= lastTimestamp && getUid(message) === record.interrupter),
      record.endTime,
      'interrupter',
    )
    if ('status' in result) return result
    interrupter = result
  }

  const suspensions: RepeatSuspensionV2[] = []
  for (const [index, suspension] of (record.suspensions ?? []).entries()) {
    const suspended = findNearest(candidates, suspension.suspendTime, `suspension ${index + 1} start`)
    if ('status' in suspended) return suspended
    const resumed = findNearest(messages, suspension.resumeTime, `suspension ${index + 1} resume`)
    if ('status' in resumed) return resumed
    suspensions.push({
      suspendMessageKey: suspended.key,
      resumeMessageKey: resumed.key,
      suspendTime: suspension.suspendTime,
      resumeTime: suspension.resumeTime,
    })
  }

  return { status: 'migrated', messages, interrupter, suspensions }
}

const createInitialState = async (ctx: Context): Promise<RepeatMigration> => ({
  id: REPEAT_MESSAGE_MIGRATION_ID,
  cursor: null,
  processed: 0,
  migrated: 0,
  unmatched: 0,
  ambiguous: 0,
  total: await ctx.database.eval('w-repeat-record', row => $.count(row.id)),
  completedAt: null,
})

export async function migrateRepeatRecords(
  ctx: Context,
  {
    batchSize = 100,
    signal,
    onProgress,
  }: RepeatMigrationOptions = {},
): Promise<RepeatMigration> {
  let [state] = await ctx.database.get('w-repeat-migration', REPEAT_MESSAGE_MIGRATION_ID)
  if (! state) {
    state = await createInitialState(ctx)
    await ctx.database.create('w-repeat-migration', state)
  }
  if (state.completedAt) return state

  while (true) {
    signal?.throwIfAborted()
    const records = await ctx.database
      .select('w-repeat-record')
      .where(state.cursor == null ? {} : { id: { $gt: state.cursor } })
      .orderBy('id')
      .limit(batchSize)
      .execute()
    if (! records.length) {
      state.completedAt = new Date()
      await ctx.database.upsert('w-repeat-migration', [state])
      onProgress?.({ state })
      return state
    }

    const existingOutcomes = await ctx.database.get('w-repeat-record-migration', {
      legacyId: { $in: records.map(record => record.id) },
    })
    const outcomeMap = new Map(existingOutcomes.map(outcome => [outcome.legacyId, outcome]))

    for (const record of records) {
      signal?.throwIfAborted()
      let outcome = outcomeMap.get(record.id)
      if (! outcome) {
        const [platform, guildId] = record.gid.split(/:(.*)/s, 2)
        const candidates = await ctx.messageDb.getMessagesByRange({
          platform,
          guildId,
          startTime: record.startTime - MATCH_TOLERANCE,
          endTime: record.endTime + MATCH_TOLERANCE,
        })
        const match = matchLegacyRepeat(record, candidates)
        outcome = {
          legacyId: record.id,
          status: match.status,
          reason: match.status === 'migrated' ? '' : match.reason,
          updatedAt: new Date(),
        }

        if (match.status === 'migrated') {
          const v2Record: RepeatRecordV2 = {
            id: record.id,
            platform,
            guildId,
            starterMessageKey: match.messages[0].key,
            interrupterMessageKey: match.interrupter?.key,
            startTime: record.startTime,
            endTime: record.endTime,
            suspensions: match.suspensions,
          }
          await ctx.database.upsert('w-repeat-record-v2', [v2Record])
          // A retry may follow a process exit between writing v2 rows and the outcome.
          // Replace the relation rows so a changed match cannot leave stale indices behind.
          await ctx.database.remove('w-repeat-message-v2', { recordId: record.id })
          await ctx.database.upsert('w-repeat-message-v2', match.messages.map((message, index) => ({
            recordId: record.id,
            index,
            messageKey: message.key,
            platform: message.platform,
            userId: message.userId,
          })))
          await ctx.messageDb.retainMessages(`w-repeat:${record.id}`, [
            ...match.messages.map(message => message.key),
            match.interrupter?.key,
            ...match.suspensions.flatMap(suspension => [
              suspension.suspendMessageKey,
              suspension.resumeMessageKey,
            ]),
          ].filter((key): key is string => !! key))
        }
        await ctx.database.upsert('w-repeat-record-migration', [outcome])
      }

      state.processed ++
      state[outcome.status] ++
      onProgress?.({
        state,
        current: {
          id: record.id,
          status: outcome.status,
          reason: outcome.reason,
        },
      })
    }

    state.cursor = records.at(-1).id
    await ctx.database.upsert('w-repeat-migration', [state])
  }
}
