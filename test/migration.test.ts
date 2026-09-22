import assert from 'node:assert/strict'
import { afterEach, describe, it } from 'node:test'

import memory from '@koishijs/plugin-database-memory'
import { Context, Service } from 'koishi'
import type { MessageRangeQuery, SavedMessage } from 'koishi-plugin-w-message-db'

import { matchLegacyRepeat, migrateRepeatRecords } from '../src/migration'
import { extendRepeatV2Models } from '../src/model-v2'

const contexts: Context[] = []

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(ctx => ctx.stop()))
})

const message = (
  id: string,
  userId: string,
  content: string,
  timestamp: number,
): SavedMessage => ({
  key: JSON.stringify(['test', '100', id]),
  id,
  platform: 'test',
  guildId: '100',
  userId,
  username: userId,
  content,
  timestamp,
  segmented: false,
  messageTypeMask: 1,
})

class MigrationMessageDb extends Service {
  messages: SavedMessage[] = []
  references = new Map<string, Set<string>>()

  constructor(ctx: Context) {
    super(ctx, 'messageDb')
  }

  async getMessagesByRange(query: MessageRangeQuery) {
    return this.messages.filter(item =>
      item.platform === query.platform &&
      item.guildId === query.guildId &&
      item.timestamp >= query.startTime &&
      item.timestamp <= query.endTime)
  }

  async retainMessages(owner: string, keys: readonly string[]) {
    this.references.set(owner, new Set(keys))
  }
}

async function createContext() {
  const ctx = new Context()
  ctx.plugin(memory)
  ctx.plugin(MigrationMessageDb)
  ctx.model.extend('w-repeat-record', {
    id: 'unsigned',
    gid: 'string',
    content: 'text',
    senders: { type: 'array', inner: 'string' },
    startTime: 'unsigned(8)',
    endTime: 'unsigned(8)',
    interrupter: 'string',
    suspensions: {
      type: 'array',
      inner: {
        type: 'object',
        inner: {
          suspendTime: 'unsigned(8)',
          resumeTime: 'unsigned(8)',
        },
      },
    },
  }, { primary: 'id' })
  extendRepeatV2Models(ctx)
  contexts.push(ctx)
  await ctx.start()
  return ctx
}

describe('repeat migration matching', () => {
  it('matches the ordered sender and content sequence', () => {
    const candidates = [
      message('1', 'u1', 'same', 1000),
      message('2', 'u2', 'same', 2000),
      message('3', 'u3', 'different', 3000),
    ]
    const result = matchLegacyRepeat({
      id: 1,
      gid: 'test:100',
      content: 'same',
      senders: ['test:u1', 'test:u2'],
      startTime: 1000,
      endTime: 3000,
      interrupter: 'test:u3',
      suspensions: [],
    }, candidates)

    assert.equal(result.status, 'migrated')
    if (result.status !== 'migrated') return
    assert.deepEqual(result.messages.map(item => item.id), ['1', '2'])
    assert.equal(result.interrupter?.id, '3')
  })

  it('matches legacy downloaded-image placeholders to message-db assets', () => {
    const result = matchLegacyRepeat({
      id: 1,
      gid: 'test:100',
      content: 'look @__KOISHI_IMG__@',
      senders: ['test:u1', 'test:u2'],
      startTime: 1000,
      endTime: 3000,
      interrupter: 'test:u3',
      suspensions: [],
    }, [
      message('1', 'u1', 'look <img src="https://assets.test/1"/>', 1000),
      message('2', 'u2', 'look <img src="https://assets.test/2"/>', 2000),
      message('3', 'u3', 'different', 3000),
    ])
    assert.equal(result.status, 'migrated')
  })

  it('does not guess between multiple valid sequences', () => {
    const result = matchLegacyRepeat({
      id: 1,
      gid: 'test:100',
      content: 'same',
      senders: ['test:u1', 'test:u2'],
      startTime: 1000,
      endTime: 3000,
      interrupter: 'test:u3',
      suspensions: [],
    }, [
      message('1a', 'u1', 'same', 1000),
      message('1b', 'u1', 'same', 1001),
      message('2', 'u2', 'same', 2000),
      message('3', 'u3', 'different', 3000),
    ])
    assert.deepEqual(result, {
      status: 'ambiguous',
      reason: 'multiple repeat message sequences',
    })
  })
})

it('migrates resumably and records unresolved rows', async () => {
  const ctx = await createContext()
  const service = ctx.messageDb as unknown as MigrationMessageDb
  service.messages.push(
    message('1', 'u1', 'same', 1000),
    message('2', 'u2', 'same', 2000),
    message('3', 'u3', 'different', 3000),
  )
  await ctx.database.upsert('w-repeat-record', [
    {
      id: 1,
      gid: 'test:100',
      content: 'same',
      senders: ['test:u1', 'test:u2'],
      startTime: 1000,
      endTime: 3000,
      interrupter: 'test:u3',
      suspensions: [],
    },
    {
      id: 2,
      gid: 'test:100',
      content: 'missing',
      senders: ['test:u1', 'test:u2'],
      startTime: 4000,
      endTime: 5000,
      interrupter: 'test:u3',
      suspensions: [],
    },
  ])

  const state = await migrateRepeatRecords(ctx, { batchSize: 1 })
  assert.equal(state.processed, 2)
  assert.equal(state.migrated, 1)
  assert.equal(state.unmatched, 1)
  assert.ok(state.completedAt)
  assert.equal((await ctx.database.get('w-repeat-record-v2', {})).length, 1)
  assert.equal((await ctx.database.get('w-repeat-message-v2', {})).length, 2)
  assert.deepEqual(
    (await ctx.database.get('w-repeat-record-migration', {}))
      .map(outcome => outcome.status)
      .sort(),
    ['migrated', 'unmatched'],
  )
  assert.deepEqual(await migrateRepeatRecords(ctx, { batchSize: 1 }), state)
  assert.equal(service.references.has('w-repeat:1'), true)
})
