import assert from 'node:assert/strict'
import { afterEach, describe, it } from 'node:test'

import memory from '@koishijs/plugin-database-memory'
import Mock from '@koishijs/plugin-mock'
import { Context, Service, type Plugin, type Session } from 'koishi'
import type { SavedMessage } from 'koishi-plugin-w-message-db'

import * as repeatPlugin from '../src'
import type { Config } from '../src'

const fixtures: Array<{ ctx: Context, mock: { dispose(): boolean } }> = []

class MessageDbMock extends Service {
  messages = new Map<string, SavedMessage>()
  references = new Map<string, Set<string>>()

  constructor(ctx: Context) {
    super(ctx, 'messageDb')
  }

  async captureMessage(session: Session): Promise<SavedMessage> {
    const key = JSON.stringify([session.platform, session.guildId, session.messageId])
    let message = this.messages.get(key)
    if (! message) {
      message = {
        key,
        id: session.messageId,
        platform: session.platform,
        guildId: session.guildId,
        userId: session.userId,
        username: session.username,
        content: session.content,
        timestamp: session.timestamp,
        quoteId: session.quote?.id,
        segmented: false,
        messageTypeMask: 1,
      }
      this.messages.set(key, message)
    }
    return message
  }

  async getMessagesByKeys(keys: readonly string[]) {
    return keys.flatMap(key => {
      const message = this.messages.get(key)
      return message ? [message] : []
    })
  }

  async getWordsByMessageKeys() { return [] }
  async retainMessages(owner: string, keys: readonly string[]) {
    this.references.set(owner, new Set(keys))
  }

  async releaseMessages(owner: string) {
    const removed = this.references.delete(owner)
    return Number(removed)
  }
}

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(async ({ ctx, mock }) => {
    mock.dispose()
    await ctx.stop()
  }))
})

async function createContext(overrides: Partial<Config['globalSettings']> = {}) {
  const ctx = new Context()
  ctx.plugin(memory)
  ctx.plugin(MessageDbMock)
  ctx.plugin(repeatPlugin, {
    isPrepend: false,
    displayLength: 25,
    imageTextDisplayLength: 15,
    displayPageSize: 10,
    ocrLangs: [ 'chi_sim', 'eng' ],
    globalSettings: {
      doWrite: true,
      repeatCount: 0,
      maxUnrelatedCount: 0,
      repeatBlacklist: [],
      doProceedImage: false,
      enableOcr: false,
      enableSegmentation: false,
      segmentationWordBlacklist: [],
      segmentationTagBlacklist: [],
      ...overrides
    },
    guildSettings: {}
  })
  const mock = ctx.plugin(Mock as Plugin.Constructor)
  fixtures.push({ ctx, mock })
  await ctx.start()
  return ctx
}

async function getRecords(ctx: Context, content?: string) {
  const records = await ctx.database.get('w-repeat-record-v2', {})
  if (! content) return records
  const messages = (ctx.messageDb as unknown as MessageDbMock).messages
  return records.filter(record => messages.get(record.starterMessageKey)?.content === content)
}

async function getRecordSenders(ctx: Context, recordId: number) {
  const rows = await ctx.database
    .select('w-repeat-message-v2', { recordId })
    .orderBy('index')
    .execute()
  return rows.map(row => `${row.platform}:${row.userId}`)
}

describe('repeat tracking', () => {
  it('increments repeatCount independently from repeatTime', async () => {
    const ctx = await createContext()
    const starter = ctx.mock.client('starter', 'group')
    const repeater = ctx.mock.client('repeater', 'group')

    await starter.receive('same', 0)
    await repeater.receive('same', 0)
    await repeater.receive('same', 0)
    await repeater.receive('same', 0)
    await starter.receive('different', 0)

    const [ user ] = await ctx.database.get('w-repeat-user', { uid: 'mock:repeater' })
    assert.equal(user.repeatTime, 1)
    assert.equal(user.repeatCount, 3)
  })

  it('does not persist a stale queued mirror after an interrupted repeat', async () => {
    const ctx = await createContext({ maxUnrelatedCount: 5 })
    for (const [ uid, content ] of [
      [ 'u1', 'A' ],
      [ 'u2', 'A' ],
      [ 'u3', 'B' ],
      [ 'u4', 'A' ],
      [ 'u5', 'D' ],
    ] as const) {
      await ctx.mock.client(uid, 'group').receive(content, 0)
    }

    const records = await getRecords(ctx, 'A')
    assert.equal(records.length, 1)
    assert.deepEqual(await getRecordSenders(ctx, records[0].id), [ 'mock:u1', 'mock:u2' ])
  })

  it('merges a suspended repeat after the content repeats again', async () => {
    const ctx = await createContext({ maxUnrelatedCount: 5 })
    for (const [ uid, content ] of [
      [ 'u1', 'A' ],
      [ 'u2', 'A' ],
      [ 'u3', 'B' ],
    ] as const) {
      await ctx.mock.client(uid, 'group').receive(content, 0)
    }
    const [suspended] = await getRecords(ctx, 'A')

    for (const [ uid, content ] of [
      [ 'u4', 'A' ],
      [ 'u5', 'A' ],
      [ 'u6', 'C' ],
    ] as const) {
      await ctx.mock.client(uid, 'group').receive(content, 0)
    }

    const records = await getRecords(ctx, 'A')
    assert.equal(records.length, 1)
    assert.equal(records[0].id, suspended.id)
    assert.deepEqual(await getRecordSenders(ctx, records[0].id), [
      'mock:u1', 'mock:u2', 'mock:u4', 'mock:u5'
    ])
    const interrupter = (ctx.messageDb as unknown as MessageDbMock)
      .messages.get(records[0].interrupterMessageKey)
    assert.equal(`${interrupter.platform}:${interrupter.userId}`, 'mock:u6')
    assert.equal(records[0].suspensions.length, 1)
  })

  it('persists the interrupted record when another candidate activates', async () => {
    const ctx = await createContext({ maxUnrelatedCount: 5 })
    for (const [ uid, content ] of [
      [ 'u1', 'A' ],
      [ 'u2', 'B' ],
      [ 'u3', 'B' ],
      [ 'u4', 'A' ],
    ] as const) {
      await ctx.mock.client(uid, 'group').receive(content, 0)
    }

    const records = await getRecords(ctx)
    assert.equal(records.length, 1)
    const starter = (ctx.messageDb as unknown as MessageDbMock)
      .messages.get(records[0].starterMessageKey)
    const interrupter = (ctx.messageDb as unknown as MessageDbMock)
      .messages.get(records[0].interrupterMessageKey)
    assert.equal(starter.content, 'B')
    assert.deepEqual(await getRecordSenders(ctx, records[0].id), [ 'mock:u2', 'mock:u3' ])
    assert.equal(`${interrupter.platform}:${interrupter.userId}`, 'mock:u4')

    const [ repeater ] = await ctx.database.get('w-repeat-user', { uid: 'mock:u4' })
    assert.equal(repeater.repeatCount, 1)
    assert.equal(repeater.repeatTime, 1)
  })

  it('reads and deletes message-backed records through commands', async () => {
    const ctx = await createContext()
    const starter = ctx.mock.client('starter', 'group')
    const repeater = ctx.mock.client('repeater', 'group')
    const interrupter = ctx.mock.client('interrupter', 'group')
    Object.assign(starter.bot, {
      getGuildMemberList: async () => ({ data: [] }),
      getGuild: async (id: string) => ({ id, name: 'Test Guild' }),
    })
    await ctx.mock.initUser('starter', 4)
    await starter.receive('saved in message-db', 0)
    await repeater.receive('saved in message-db', 0)
    await interrupter.receive('different', 0)

    const [record] = await ctx.database.get('w-repeat-record-v2', {})
    const service = ctx.messageDb as unknown as MessageDbMock
    assert.equal(service.references.has(`w-repeat:${record.id}`), true)

    await ctx.database.upsert('w-repeat-record', [{
      id: record.id,
      gid: 'mock:group',
      content: 'saved in message-db',
      senders: ['mock:starter', 'mock:repeater'],
      startTime: record.startTime,
      endTime: record.endTime,
      interrupter: 'mock:interrupter',
      quote: null,
      images: [],
      words: [],
      suspensions: [],
    }])
    await ctx.database.upsert('w-repeat-record-migration', [{
      legacyId: record.id,
      status: 'migrated',
      reason: '',
      updatedAt: new Date(),
    }])

    const [stats] = await starter.receive('repeat.stat -d all', 1)
    assert.match(stats, /saved in message-db/)
    assert.match(stats, /共有 1 次/)

    const [detail] = await starter.receive(`repeat.record ${record.id}`, 1)
    assert.match(detail, /saved in message-db/)
    assert.match(detail, /2 个/)

    const [deleted] = await starter.receive(`repeat.record ${record.id} -d`, 1)
    assert.match(deleted, /\[已删除\]/)
    assert.deepEqual(await ctx.database.get('w-repeat-record-v2', { id: record.id }), [])
    assert.deepEqual(await ctx.database.get('w-repeat-message-v2', { recordId: record.id }), [])
    assert.deepEqual(await ctx.database.get('w-repeat-record', { id: record.id }), [])
    assert.equal(service.references.has(`w-repeat:${record.id}`), false)
  })
})

describe('repeat word commands', () => {
  it('filters tags by guild and the tag blacklist', async () => {
    const ctx = await createContext({ segmentationTagBlacklist: [ 'x' ] })
    await ctx.database.upsert('w-repeat-word', [
      { gid: 'mock:group', word: 'local', tag: 'n', count: 3 },
      { gid: 'mock:group', word: 'blocked', tag: 'x', count: 10 },
      { gid: 'mock:other', word: 'foreign', tag: 'v', count: 20 },
    ])

    const client = ctx.mock.client('user', 'group')
    const [ reply ] = await client.receive('repeat.word.tag', 1)
    assert.match(reply, /n: 3/)
    assert.doesNotMatch(reply, /x: 10/)
    assert.doesNotMatch(reply, /v: 20/)

    const [ unfiltered ] = await client.receive('repeat.word.tag --all', 1)
    assert.match(unfiltered, /x: 10/)
    assert.doesNotMatch(unfiltered, /v: 20/)
  })
})
