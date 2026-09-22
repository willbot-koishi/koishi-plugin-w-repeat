import assert from 'node:assert/strict'
import { afterEach, it } from 'node:test'

import memory from '@koishijs/plugin-database-memory'
import { Context } from 'koishi'

import { extendRepeatV2Models } from '../src/model-v2'

const contexts: Context[] = []

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(ctx => ctx.stop()))
})

async function createContext() {
  const ctx = new Context()
  ctx.plugin(memory)
  extendRepeatV2Models(ctx)
  contexts.push(ctx)
  await ctx.start()
  return ctx
}

it('stores repeat metadata separately from message content', async () => {
  const ctx = await createContext()
  await ctx.database.create('w-repeat-record-v2', {
    id: 170728,
    platform: 'onebot',
    guildId: '100',
    starterMessageKey: '["onebot","100","1"]',
    interrupterMessageKey: '["onebot","100","4"]',
    startTime: 1,
    endTime: 4,
    suspensions: [],
  })
  await ctx.database.upsert('w-repeat-message-v2', [
    {
      recordId: 170728,
      index: 0,
      messageKey: '["onebot","100","1"]',
      platform: 'onebot',
      userId: '10',
    },
    {
      recordId: 170728,
      index: 1,
      messageKey: '["onebot","100","2"]',
      platform: 'onebot',
      userId: '20',
    },
  ])

  const [record] = await ctx.database.get('w-repeat-record-v2', 170728)
  const messages = await ctx.database.get('w-repeat-message-v2', { recordId: 170728 })
  assert.equal(record.starterMessageKey, '["onebot","100","1"]')
  assert.equal(messages.length, 2)
  assert.deepEqual(messages.map(message => message.index), [0, 1])
  assert.equal('content' in record, false)
})
