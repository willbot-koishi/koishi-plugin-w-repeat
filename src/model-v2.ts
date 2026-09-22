import type { Context } from 'koishi'

export interface RepeatSuspensionV2 {
  suspendMessageKey: string
  resumeMessageKey: string
  suspendTime: number
  resumeTime: number
}

export interface RepeatRecordV2 {
  id: number
  platform: string
  guildId: string
  starterMessageKey: string
  interrupterMessageKey?: string
  startTime: number
  endTime: number
  suspensions: RepeatSuspensionV2[]
}

export interface RepeatMessageV2 {
  recordId: number
  index: number
  messageKey: string
  platform: string
  userId: string
}

declare module 'koishi' {
  interface Tables {
    'w-repeat-record-v2': RepeatRecordV2
    'w-repeat-message-v2': RepeatMessageV2
  }
}

export function extendRepeatV2Models(ctx: Context) {
  ctx.model.extend('w-repeat-record-v2', {
    id: 'unsigned',
    platform: 'string',
    guildId: 'string',
    starterMessageKey: 'string',
    interrupterMessageKey: 'string',
    startTime: 'unsigned(8)',
    endTime: 'unsigned(8)',
    suspensions: {
      type: 'array',
      inner: {
        type: 'object',
        inner: {
          suspendMessageKey: 'string',
          resumeMessageKey: 'string',
          suspendTime: 'unsigned(8)',
          resumeTime: 'unsigned(8)',
        },
      },
    },
  }, {
    primary: 'id',
    indexes: [
      ['platform', 'guildId', 'startTime'],
    ],
  })

  ctx.model.extend('w-repeat-message-v2', {
    recordId: 'unsigned',
    index: 'unsigned',
    messageKey: 'string',
    platform: 'string',
    userId: 'string',
  }, {
    primary: ['recordId', 'index'],
    indexes: [
      ['messageKey'],
      ['platform', 'userId', 'recordId'],
    ],
  })
}
