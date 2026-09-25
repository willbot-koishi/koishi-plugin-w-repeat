import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
  createRepeatTrackerState,
  suspendPersistedCandidate,
  transitionRepeat,
  type TrackedMessage,
} from '../src/tracker'

const message = (index: number, content: string): TrackedMessage => ({
  key: `message-${index}`,
  platform: 'test',
  guildId: '100',
  userId: `user-${index}`,
  content,
  timestamp: index,
})

const options = { maxUnrelatedCount: 5, repeatCount: 3 }

describe('repeat tracker', () => {
  it('completes an interrupted repeat and activates a queued candidate', () => {
    const state = createRepeatTrackerState()
    transitionRepeat(state, message(1, 'A'), options)
    transitionRepeat(state, message(2, 'B'), options)
    transitionRepeat(state, message(3, 'B'), options)
    const result = transitionRepeat(state, message(4, 'A'), options)

    assert.equal(result.completed?.messages[0].content, 'B')
    assert.deepEqual(result.completed?.messages.map(item => item.key), [
      'message-2', 'message-3',
    ])
    assert.equal(result.current.messages[0].content, 'A')
    assert.deepEqual(result.current.messages.map(item => item.key), [
      'message-1', 'message-4',
    ])
  })

  it('resumes a persisted repeat on the first matching message', () => {
    const state = createRepeatTrackerState()
    transitionRepeat(state, message(1, 'A'), options)
    transitionRepeat(state, message(2, 'A'), options)
    const interrupted = transitionRepeat(state, message(3, 'B'), options).completed
    suspendPersistedCandidate(state, interrupted, 10)

    const result = transitionRepeat(state, message(4, 'A'), options)

    assert.equal(result.resumedRecordId, 10)
    assert.equal(result.current.persistedId, 10)
    assert.deepEqual(result.current.messages.map(item => item.key), [
      'message-1', 'message-2', 'message-4',
    ])
    assert.deepEqual(result.current.suspensions, [{
      suspendMessageKey: 'message-3',
      suspendTime: 3,
      resumeMessageKey: 'message-4',
      resumeTime: 4,
    }])
    assert.equal(result.shouldRepeat, true)
  })

  it('completes the current repeat while resuming a suspended one', () => {
    const state = createRepeatTrackerState()
    transitionRepeat(state, message(1, 'A'), options)
    transitionRepeat(state, message(2, 'A'), options)
    const interrupted = transitionRepeat(state, message(3, 'B'), options).completed
    suspendPersistedCandidate(state, interrupted, 10)
    transitionRepeat(state, message(4, 'B'), options)

    const result = transitionRepeat(state, message(5, 'A'), options)

    assert.equal(result.resumedRecordId, 10)
    assert.deepEqual(result.current.messages.map(item => item.key), [
      'message-1', 'message-2', 'message-5',
    ])
    assert.deepEqual(result.completed?.messages.map(item => item.key), [
      'message-3', 'message-4',
    ])
  })

  it('only requests a bot repeat at the configured count', () => {
    const state = createRepeatTrackerState()
    assert.equal(transitionRepeat(state, message(1, 'A'), options).shouldRepeat, false)
    assert.equal(transitionRepeat(state, message(2, 'A'), options).shouldRepeat, false)
    assert.equal(transitionRepeat(state, message(3, 'A'), options).shouldRepeat, true)
    assert.equal(transitionRepeat(state, message(4, 'A'), options).shouldRepeat, false)
  })
})
