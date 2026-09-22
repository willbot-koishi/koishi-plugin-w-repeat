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

export type RepeatMigrationStatus = 'migrated' | 'unmatched' | 'ambiguous'

export interface RepeatRecordMigration {
  legacyId: number
  status: RepeatMigrationStatus
  reason: string
  updatedAt: Date
}

export interface RepeatMigration {
  id: string
  cursor?: number
  processed: number
  migrated: number
  unmatched: number
  ambiguous: number
  total: number
  completedAt?: Date
}

declare module 'koishi' {
  interface Tables {
    'w-repeat-record-v2': RepeatRecordV2
    'w-repeat-message-v2': RepeatMessageV2
    'w-repeat-record-migration': RepeatRecordMigration
    'w-repeat-migration': RepeatMigration
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

  ctx.model.extend('w-repeat-record-migration', {
    legacyId: 'unsigned',
    status: 'string',
    reason: 'text',
    updatedAt: 'timestamp',
  }, {
    primary: 'legacyId',
    indexes: [
      ['status'],
    ],
  })

  ctx.model.extend('w-repeat-migration', {
    id: 'string',
    cursor: { type: 'unsigned', nullable: true, initial: null },
    processed: { type: 'unsigned', length: 8, initial: 0 },
    migrated: { type: 'unsigned', length: 8, initial: 0 },
    unmatched: { type: 'unsigned', length: 8, initial: 0 },
    ambiguous: { type: 'unsigned', length: 8, initial: 0 },
    total: { type: 'unsigned', length: 8, initial: 0 },
    completedAt: { type: 'timestamp', nullable: true, initial: null },
  }, {
    primary: 'id',
  })
}
