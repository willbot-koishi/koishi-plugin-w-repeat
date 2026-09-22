export interface TrackedMessage {
  key: string
  platform: string
  guildId: string
  userId: string
  content: string
  timestamp: number
  quoteId?: string
}

export interface TrackedSuspension {
  suspendMessageKey: string
  resumeMessageKey: string
  suspendTime: number
  resumeTime: number
}

export interface RepeatCandidate {
  messages: TrackedMessage[]
  startTime: number
  endTime?: number
  interrupter?: TrackedMessage
  suspensions: TrackedSuspension[]
  quoteId?: string
  quoteValid: boolean
  unrelatedCount: number
  persistedId?: number
  pendingSuspension?: {
    suspendMessageKey: string
    suspendTime: number
  }
}

export interface RepeatTrackerState {
  current?: RepeatCandidate
  queued: RepeatCandidate[]
  suspended: RepeatCandidate[]
}

export interface RepeatTransition {
  current: RepeatCandidate
  completed?: RepeatCandidate
  resumedRecordId?: number
  stats?: RepeatCandidate
  shouldRepeat: boolean
}

export interface RepeatTrackerOptions {
  maxUnrelatedCount: number
  repeatCount: number
}

export const createRepeatTrackerState = (): RepeatTrackerState => ({
  queued: [],
  suspended: [],
})

const isSameMessage = (message: TrackedMessage, candidate?: RepeatCandidate) =>
  !! candidate && message.content === candidate.messages[0]?.content

const updateQuote = (candidate: RepeatCandidate, message: TrackedMessage) => {
  if (! message.quoteId) return
  if (! candidate.quoteId) {
    candidate.quoteId = message.quoteId
    candidate.quoteValid = true
  }
  else if (candidate.quoteId !== message.quoteId) {
    candidate.quoteValid = false
  }
}

const appendMessage = (candidate: RepeatCandidate, message: TrackedMessage) => {
  candidate.messages.push(message)
  candidate.unrelatedCount = 0
  updateQuote(candidate, message)
}

const createCandidate = (message: TrackedMessage): RepeatCandidate => {
  const candidate: RepeatCandidate = {
    messages: [message],
    startTime: message.timestamp,
    suspensions: [],
    quoteValid: true,
    unrelatedCount: 0,
  }
  updateQuote(candidate, message)
  return candidate
}

export function transitionRepeat(
  state: RepeatTrackerState,
  message: TrackedMessage,
  options: RepeatTrackerOptions,
): RepeatTransition {
  let current = state.current
  const isRepeating = isSameMessage(message, current)
  let resumedRecordId: number | undefined
  let didResume = false

  for (const candidate of state.suspended) {
    if (isSameMessage(message, candidate)) {
      appendMessage(candidate, message)
      if (isRepeating && ! didResume) {
        const resumeCandidate = current
        const pending = candidate.pendingSuspension
        if (pending) {
          candidate.suspensions.push({
            ...pending,
            resumeMessageKey: message.key,
            resumeTime: message.timestamp,
          })
        }
        resumedRecordId = candidate.persistedId
        delete candidate.pendingSuspension
        state.queued = state.queued.filter(item => item !== resumeCandidate)
        current = state.current = candidate
        didResume = true
      }
    }
    else if (++ candidate.unrelatedCount > options.maxUnrelatedCount) {
      candidate.unrelatedCount = Number.POSITIVE_INFINITY
    }
  }
  state.suspended = state.suspended.filter(candidate =>
    candidate.unrelatedCount !== Number.POSITIVE_INFINITY && candidate !== current)

  let completed: RepeatCandidate | undefined
  let stats: RepeatCandidate | undefined
  if (isRepeating) {
    if (! didResume) appendMessage(current, message)
    if (current.messages.length > 1) stats = current
  }
  else {
    const interrupted = current
    let activated: RepeatCandidate | undefined

    for (const candidate of state.queued) {
      if (isSameMessage(message, candidate)) {
        appendMessage(candidate, message)
        if (! activated) activated = candidate
        else candidate.unrelatedCount = Number.POSITIVE_INFINITY
      }
      else if (++ candidate.unrelatedCount > options.maxUnrelatedCount) {
        candidate.unrelatedCount = Number.POSITIVE_INFINITY
      }
    }
    state.queued = state.queued.filter(candidate =>
      candidate.unrelatedCount !== Number.POSITIVE_INFINITY && candidate !== activated)

    if (activated) {
      current = state.current = activated
      if (current.messages.length > 1) stats = current
    }

    if (interrupted && interrupted.messages.length > 1) {
      state.queued = state.queued.filter(candidate => candidate !== interrupted)
      interrupted.interrupter = message
      interrupted.endTime = message.timestamp
      completed = interrupted
      if (options.maxUnrelatedCount) {
        interrupted.pendingSuspension = {
          suspendMessageKey: message.key,
          suspendTime: message.timestamp,
        }
      }
    }

    if (! activated) {
      current = state.current = createCandidate(message)
      state.queued.unshift(current)
    }
  }

  return {
    current,
    completed,
    resumedRecordId,
    stats,
    shouldRepeat: options.repeatCount > 0 && current.messages.length === options.repeatCount,
  }
}

export function suspendPersistedCandidate(
  state: RepeatTrackerState,
  candidate: RepeatCandidate,
  recordId: number,
) {
  if (! candidate.pendingSuspension) return
  candidate.persistedId = recordId
  state.suspended.unshift(candidate)
}
