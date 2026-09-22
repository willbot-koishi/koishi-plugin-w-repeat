import assert from 'node:assert/strict'
import { afterEach, describe, it } from 'node:test'

import memory from '@koishijs/plugin-database-memory'
import Mock from '@koishijs/plugin-mock'
import { Context, type Plugin } from 'koishi'

import * as repeatPlugin from '../src'
import type { Config } from '../src'

const fixtures: Array<{ ctx: Context, mock: { dispose(): boolean } }> = []

afterEach(async () => {
  await Promise.all(fixtures.splice(0).map(async ({ ctx, mock }) => {
    mock.dispose()
    await ctx.stop()
  }))
})

async function createContext(overrides: Partial<Config['globalSettings']> = {}) {
  const ctx = new Context()
  ctx.plugin(memory)
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

    const records = await ctx.database.get('w-repeat-record', { content: 'A' })
    assert.equal(records.length, 1)
    assert.deepEqual(records[0].senders, [ 'mock:u1', 'mock:u2' ])
  })

  it('merges a suspended repeat after the content repeats again', async () => {
    const ctx = await createContext({ maxUnrelatedCount: 5 })
    for (const [ uid, content ] of [
      [ 'u1', 'A' ],
      [ 'u2', 'A' ],
      [ 'u3', 'B' ],
      [ 'u4', 'A' ],
      [ 'u5', 'A' ],
      [ 'u6', 'C' ],
    ] as const) {
      await ctx.mock.client(uid, 'group').receive(content, 0)
    }

    const records = await ctx.database.get('w-repeat-record', { content: 'A' })
    assert.equal(records.length, 1)
    assert.deepEqual(records[0].senders, [
      'mock:u1', 'mock:u2', 'mock:u4', 'mock:u5'
    ])
    assert.equal(records[0].interrupter, 'mock:u6')
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

    const records = await ctx.database.get('w-repeat-record', {})
    assert.equal(records.length, 1)
    assert.equal(records[0].content, 'B')
    assert.deepEqual(records[0].senders, [ 'mock:u2', 'mock:u3' ])
    assert.equal(records[0].interrupter, 'mock:u4')

    const [ repeater ] = await ctx.database.get('w-repeat-user', { uid: 'mock:u4' })
    assert.equal(repeater.repeatCount, 1)
    assert.equal(repeater.repeatTime, 1)
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
