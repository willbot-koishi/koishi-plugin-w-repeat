import {
  h, z,
  Argv, Context, Session, SessionError,
  $, Direction, Query,
  Fragment,
  Awaitable,
  Update,
  type Tables,
} from 'koishi'
import { type GuildMember } from '@satorijs/protocol'

import {} from '@koishijs/plugin-help'
import {} from 'koishi-plugin-w-option-conflict'
import { type echarts } from 'koishi-plugin-w-echarts'
import type { SavedMessage } from 'koishi-plugin-w-message-db'
import {} from 'koishi-plugin-w-tesseract'
import { type Jieba, type Tag } from 'koishi-plugin-w-jieba'

import dedent from 'dedent'
import dayjs, { type Dayjs } from 'dayjs'

import { extendRepeatV2Models, type RepeatRecordV2 } from './model-v2'
import { migrateRepeatRecords, REPEAT_MESSAGE_MIGRATION_ID } from './migration'
import {
  createRepeatTrackerState,
  suspendPersistedCandidate,
  transitionRepeat,
  type RepeatCandidate,
  type RepeatTrackerState,
  type TrackedMessage,
} from './tracker'

export const name = 'w-repeat'

export const inject = {
  required: ['database', 'messageDb'],
  optional: ['echarts', 'tesseract', 'jieba']
}

export interface RepeatSettings {
  doWrite: boolean

  repeatCount: number
  maxUnrelatedCount: number

  repeatBlacklist: string[]

  doProceedImage: boolean
  enableOcr: boolean

  enableSegmentation: boolean
  segmentationWordBlacklist: string[]
  segmentationTagBlacklist: string[]
}

export const RepeatSettings: z<RepeatSettings> = z.object({
  doWrite: z.boolean().default(true).description('是否启用复读写入（包括记录和机器人复读，不包括查询）'),

  repeatCount: z.natural().default(0).description('机器人复读需要的次数，0 为不复读'),
  maxUnrelatedCount: z.natural().default(5).description('恢复挂起的复读前允许的最大无关消息条数，0 为禁用挂起'),

  repeatBlacklist: z.array(z.string()).description('复读内容黑名单'),

  doProceedImage: z.boolean().default(false).description('旧版图片处理开关（新记录由 message-db 转存资源）'),
  enableOcr: z.boolean().default(true).description('是否为旧版复读记录生成 OCR 结果'),

  enableSegmentation: z.boolean().default(true).description('是否更新复读词频（旧记录本地分词，新记录使用 message-db 分词）'),
  segmentationWordBlacklist: z.array(z.string()).description('复读词频统计中不显示的词'),
  segmentationTagBlacklist: z.array(z.string()).description('复读词频统计中不显示的词性'),
})

export interface Config {
  isPrepend: boolean

  displayLength: number
  imageTextDisplayLength: number
  displayPageSize: number

  ocrLangs: string[]

  globalSettings: RepeatSettings
  guildSettings: Record<string, RepeatSettings>
}

export const Config: z<Config> = z.object({
  isPrepend: z.boolean().default(false).description('是否注册为前置中间件'),

  globalSettings: RepeatSettings.description('全局复读设置'),
  guildSettings: z.dict(RepeatSettings).description('群复读设置'),

  displayLength: z.natural().default(25).description('复读消息最大长度，超过则显示为省略号'),
  imageTextDisplayLength: z.natural().default(15).description('图片文字预览最大长度，超过则显示为省略号'),
  displayPageSize: z.natural().min(5).default(10).description('复读消息分页大小，至少为 5'),

  ocrLangs: z.array(z.string()).default([ 'chi_sim', 'eng' ]).description('识别图片中文字时尝试的语言，参考 ' +
    '<https://tesseract-ocr.github.io/tessdoc/Data-Files#data-files-for-version-400-november-29-2016>'
  ),
})

declare module 'koishi' {
  interface Tables {
    'w-repeat-record': RepeatRecord     // 复读记录表
    'w-repeat-user': RepeatUser       // 复读用户表
    'w-repeat-calendar': RepeatDay      // 复读日历
    'w-repeat-word': RepeatWord       // 复读分词表
  }
}

export interface RepeatImage {
  b64: string
  text: string
}

export interface RepeatMessage {
  content?: string
  images?: RepeatImage[]
  words?: Tag[]
  quote?: {
    valid: boolean
    content?: string
    id?: string
  }
}

export interface RepeatRecordBase extends RepeatMessage {
  gid: string
  senders: string[]
  startTime: number
  endTime: number
  interrupter: string
  suspensions: RepeatSuspensionBase[]
}

export interface RepeatRecord extends RepeatRecordBase {
  id: number
}

export interface RepeatSuspensionBase {
  suspendTime: number
  resumeTime: number
}

export interface RepeatWindow {
  unrelatedCount: number
}

export interface RepeatSuspension extends RepeatSuspensionBase, RepeatWindow {}

export interface Deletable {
  deleted?: boolean
}

export interface RepeatSuspendedRecord extends RepeatRecord, RepeatSuspension, Deletable {}

export interface RepeatQueuedRecord extends RepeatRecordBase, RepeatWindow, Deletable {}

export interface RepeatRuntime {
  currentRec: RepeatQueuedRecord | RepeatSuspendedRecord
  queuedRecs: RepeatQueuedRecord[]
  suspendedRecs: RepeatSuspendedRecord[]
}

export interface RepeatUser {
  uid: string
  repeatTime: number
  repeatCount: number
  beRepeatedTime: number
  beRepeatedCount: number
  interruptTime: number
}

export interface RepeatDay {
  gid: string
  month: string
  day: string
  topRepeaterId: string
  topRepeaterCount: number
  topStarterId: string
  topStarterCount: number
  topInterrupterId: string
  topInterrupterCount: number
}

export interface RepeatWord {
  gid: string
  word: string
  tag: string
  count: number
}

export async function apply(ctx: Context, config: Config) {
  // 扩展数据库模型
  extendRepeatV2Models(ctx)
  ctx.model.extend('w-repeat-record', {
    id: 'unsigned',
    gid: 'string',
    content: 'text',
    senders: {
      type: 'array',
      inner: 'string'
    },
    startTime: 'unsigned',
    endTime: 'unsigned',
    interrupter: 'string',
    quote: {
      type: 'object',
      inner: {
        valid: 'boolean',
        content: 'string',
        id: 'string'
      }
    },
    images: {
      type: 'array',
      inner: {
        type: 'object',
        inner: {
          text: 'string',
          b64: 'string'
        }
      }
    },
    words: {
      type: 'array',
      inner: {
        type: 'object',
        inner: {
          tag: 'string',
          word: 'string'
        }
      }
    },
    suspensions: {
      type: 'array',
      inner: {
        type: 'object',
        inner: {
          suspendTime: 'unsigned',
          resumeTime: 'unsigned'
        }
      }
    }
  }, {
    autoInc: true,
    indexes: [[ 'gid', 'startTime' ]]
  })

  const counterField = () => ({
    type: 'unsigned',
    initial: 0
  } as const)
  ctx.model.extend('w-repeat-user', {
    uid: 'string',
    repeatTime: counterField(),
    repeatCount: counterField(),
    beRepeatedTime: counterField(),
    beRepeatedCount: counterField(),
    interruptTime: counterField()
  }, { primary: 'uid' })

  ctx.model.extend('w-repeat-calendar', {
    gid: 'string',
    month: 'string',
    day: 'string',
    topRepeaterId: 'string',
    topRepeaterCount: 'unsigned',
    topStarterId: 'string',
    topStarterCount: 'unsigned',
    topInterrupterId: 'string',
    topInterrupterCount: 'unsigned'
  }, {
    primary: [ 'gid', 'month', 'day' ]
  })

  ctx.model.extend('w-repeat-word', {
    gid: 'string',
    word: 'string',
    tag: 'string',
    count: 'unsigned'
  }, {
    primary: [ 'gid', 'word', 'tag' ]
  })

  // Tesseract 初始化

  let tesseractWorker: Tesseract.Worker = undefined
  const initTesseract = async () => {
    if (! ctx.tesseract) return
    try {
      const { ocrLangs } = config
      await ctx.tesseract.installNecessaryLangs(ocrLangs)
      tesseractWorker = await ctx.tesseract.createWorker(ocrLangs)
      ctx.logger.debug('inited Tesseract.')
    }
    catch (error) {
      ctx.logger.error('failed to init Tesseract: %o', error)
    }
  }

  // Jieba 初始化

  let jieba: Jieba = undefined
  const initJieba = async () => {
    if (! ctx.jieba) return
    try {
      jieba = new ctx.jieba.Jieba
      ctx.logger.debug('inited Jieba.')
    }
    catch (error) {
      ctx.logger.error('failed to init Jieba: %o', error)
    }
  }

  // 等待所有初始化完成

  await Promise.all([
    initTesseract(),
    initJieba()
  ])

  // 工具函数
  // 配置
  const getGuildSettings = (gid: string) => config.guildSettings[gid] ?? config.globalSettings

  // String
  const ellipsis = (s: string, maxLength: number): string => {
    const els = h.parse(s)
    let length = 0
    let output = ''
    for (const el of els) {
      if (el.type === 'text') {
        const text = el.toString()
        length += text.length
        if (length > maxLength) {
          output += text.slice(0, text.length - (length - maxLength) - 3) + '...'
          break
        }
        output += text
      }
      else output += el.toString() // TODO: length of other elements
    }
    return output
  }

  const capitalize = <S extends string>(s: S): Capitalize<S> =>
    (s[0].toUpperCase() + s.slice(1)) as any

  const timeText = (time: number) => dayjs(time).format('YYYY/MM/DD HH:mm:ss')

  const splitWithLimit = (s: string, delimiter: string, limit: number) => {
    const segs = s.split(delimiter)
    const initSegs = segs.slice(0, limit - 1)
    const lastSeg = segs.slice(limit - 1).join(delimiter)
    return [ ...initSegs, lastSeg ]
  }

  // Array
  const maybeArray = <T>(x: T | T[]): T[] => Array.isArray(x) ? x : [ x ]

  const countBy = <T extends {}, K extends keyof any>(xs: T[], key: (x: T) => K | K[]) =>
    xs.reduce<Record<keyof any, number>>((dict, x) => {
      maybeArray(key(x)).forEach(k => {
        dict[k] ??= 0
        dict[k] ++
      })
      return dict
    }, {})

  const elem = <T, U extends T>(x: T, xs: U[]): x is U => xs.includes(x as any)

  const countAndSortBy = <T extends {}, K extends keyof any>(xs: T[], key: (x: T) => K | K[]) =>
    Object.entries(countBy(xs, key)).sort(([, count1 ], [, count2 ]) => count2 - count1)

  const exclude = <T>(xs: T[], ys: T[]): T[] => {
    const yset = new Set(ys)
    return xs.filter(x => ! yset.has(x))
  }

  // Dict
  const pick = <T extends {}, K extends keyof T>(x: T, keys: K[]): Pick<T, K> =>
    Object.fromEntries(Object.entries(x).filter(([ k ]) => keys.includes(k as any))) as any

  const omit = <T extends {}, K extends keyof T>(x: T, keys: K[]): Omit<T, K> =>
    Object.fromEntries(Object.entries(x).filter(([ k ]) => ! keys.includes(k as any))) as any

  const safeInc = <K extends keyof any>(o: Record<K, number>) => (k: K) => {
    o[k] ??= 0
    o[k] ++
  }

  // Promise
  const unzipPromise = async <T, U>(p: Promise<readonly [T, U][]>) => {
    const xs: T[] = [], ys: U[] = []
    for (const [ x, y ] of await p) {
      xs.push(x)
      ys.push(y)
    }
    return [ xs, ys ] as const
  }

  // Date
  const getDaysOfMonth = (date: Dayjs = dayjs()) => {
    const days: string[] = []
    const month = date.get('month')
    for (let day = date.startOf('month'); day.get('month') === month; day = day.add(1, 'day'))
      days.push(day.format('DD'))
    return days
  }

  // Adapter
  const getMemberDict = async (session: Session, guildId: string) => {
    const { data: memberList } = await session.bot.getGuildMemberList(guildId)
    const dict: Record<string, GuildMember> = {}
    for (const member of memberList)
      dict[`${session.platform}:${member.user.id}`] = member
    return dict
  }

  const getMemberName = (memberDict: Record<string, GuildMember>, uid: string) => {
    const member = memberDict?.[uid]
    return member ? (member.nick || member.name || member.user.name || member.user.id) : uid
  }

  // Command
  const requireList = (): Argv.OptionConfig => ({
    conflictsWith: { option: 'list', value: false }
  })

  const profile = async (fn: () => Awaitable<void | Fragment>): Promise<Fragment> => {
    const start = Date.now()
    const res = await fn()
    const time = Date.now() - start
    return `${res ?? ''}\n\n用时 ${time > 2000
      ? (time * .001).toFixed(3) + 's'
      : time.toFixed(3) + 'ms'
    }`
  }

  const parseDuration = (duration: string): Query<RepeatRecord> & object => {
    if (duration === 'all') return {}
    else if (elem(duration, [ 'hour', 'day', 'week', 'month' ] as const)) return {
      startTime: { $gte: + dayjs().startOf(duration) }
    }
    else if (duration.includes('~')) {
      const [ start, end ] = duration
        .split('~')
        .map(str => {
          str = str.trim()
          if (! str) return undefined

          const date = dayjs(str)
          if (! date.isValid()) throw new SessionError(`'${str}' 不是有效的时间`)
          return date
        })
      return {
        startTime: start ? { $gte: + start } : {},
        endTime: end ? { $lte: + end } : {}
      }
    }
    throw new SessionError(`'${duration}' 不是有效的时间范围`)
  }

  const $inc = (expr: $.Expr) => $.add(expr, 1)

  // Repeat
  const updateImageText = async (rec: RepeatRecord | RepeatQueuedRecord) => {
    const images = rec.images ??= []

    await Promise.all(images.filter(x => x !== null).map(async ({ b64 }, i) => {
      const res = await tesseractWorker.recognize(Buffer.from(b64, 'base64'))
      const { text } = res.data
      images[i].text = text
    }))

    if ('id' in rec) await ctx.database.set('w-repeat-record', { id: rec.id }, { images })
  }

  const updateWords = async (rec: RepeatRecord | RepeatQueuedRecord) => {
    const text = h.transform(
      (rec.content ?? '').replace(/@__KOISHI_IMG__@/g, ''),
      el => el.type === 'text' ? el.toString() : ''
    )
    const words = rec.words = jieba
      .tag(text)

    if ('id' in rec) await ctx.database.set('w-repeat-record', { id: rec.id }, { words })

    return words
  }

  const unescapeMessage = (
    message: RepeatMessage,
    {
      allowImage = true,
      allowFace = true
    }: {
      allowImage?: boolean,
      allowFace?: boolean
    } = {}
  ): string => {
    let imageIdx = 0
    const images = message.images ?? []
    let content = (message.content ?? '').replace(
      /@__KOISHI_IMG__@/g,
      () => {
        const image = images[imageIdx ++]
        if (! image) return '[图片加载失败]'
        return allowImage
          ? h.img('data:image/png;base64,' + image.b64).toString()
          : `[图片${ image.text ? ': ' + image.text.replace(/\s+/g, ' ') : '' }]`
      }
    )
    content = h.transform(content, {
      face: allowFace ? undefined : () => '[表情]'
    })
    return content
  }

  type UnifiedRepeatRecord = RepeatRecord & {
    storageVersion: 1 | 2
    hasImage: boolean
  }

  const hydrateV2Records = async (records: RepeatRecordV2[]): Promise<UnifiedRepeatRecord[]> => {
    if (! records.length) return []
    const recordIds = records.map(record => record.id)
    const rows: Tables['w-repeat-message-v2'][] = []
    for (let index = 0; index < recordIds.length; index += 500) {
      rows.push(...await ctx.database.get('w-repeat-message-v2', {
        recordId: { $in: recordIds.slice(index, index + 500) },
      }))
    }
    const keys = [...new Set([
      ...records.flatMap(record => [record.starterMessageKey, record.interrupterMessageKey]),
      ...rows.map(row => row.messageKey),
    ].filter((key): key is string => !! key))]
    const [messages, words] = await Promise.all([
      ctx.messageDb.getMessagesByKeys(keys),
      ctx.messageDb.getWordsByMessageKeys(records.map(record => record.starterMessageKey)),
    ])
    const messageMap = new Map(messages.map(message => [message.key, message]))
    const rowsByRecord = new Map<number, typeof rows>()
    for (const row of rows) {
      const recordRows = rowsByRecord.get(row.recordId) ?? []
      recordRows.push(row)
      rowsByRecord.set(row.recordId, recordRows)
    }
    const wordsByMessage = new Map<string, typeof words>()
    for (const word of words) {
      const messageWords = wordsByMessage.get(word.messageKey) ?? []
      messageWords.push(word)
      wordsByMessage.set(word.messageKey, messageWords)
    }

    return records.flatMap(record => {
      const starter = messageMap.get(record.starterMessageKey)
      if (! starter) {
        ctx.logger.warn('repeat record %d references a missing starter message', record.id)
        return []
      }
      const recordRows = (rowsByRecord.get(record.id) ?? []).sort((a, b) => a.index - b.index)
      const interrupter = messageMap.get(record.interrupterMessageKey)
      return [{
        id: record.id,
        gid: `${record.platform}:${record.guildId}`,
        content: starter.content,
        senders: recordRows.map(row => `${row.platform}:${row.userId}`),
        startTime: record.startTime,
        endTime: record.endTime,
        interrupter: interrupter ? `${interrupter.platform}:${interrupter.userId}` : '',
        suspensions: record.suspensions.map(({ suspendTime, resumeTime }) => ({
          suspendTime,
          resumeTime,
        })),
        words: wordsByMessage.get(record.starterMessageKey),
        storageVersion: 2,
        hasImage: /<(?:img|image)\b/i.test(starter.content),
      }]
    })
  }

  const getV2Records = async (
    gid?: string,
    duration: Record<string, any> = {},
  ) => {
    const query: Record<string, any> = { ...duration }
    if (gid) {
      const [platform, guildId] = splitWithLimit(gid, ':', 2)
      query.platform = platform
      query.guildId = guildId
    }
    return hydrateV2Records(await ctx.database.get('w-repeat-record-v2', query))
  }

  const asUnifiedV1 = (record: RepeatRecord): UnifiedRepeatRecord => ({
    ...record,
    storageVersion: 1,
    hasImage: !! record.images?.length,
  })

  const filterUnmigratedLegacy = async <T extends { id: number }>(records: T[]): Promise<T[]> => {
    if (! records.length) return records
    const migratedIds = new Set<number>()
    for (let index = 0; index < records.length; index += 500) {
      const outcomes = await ctx.database.get('w-repeat-record-migration', {
        legacyId: { $in: records.slice(index, index + 500).map(record => record.id) },
        status: 'migrated',
      })
      outcomes.forEach(outcome => migratedIds.add(outcome.legacyId))
    }
    return records.filter(record => ! migratedIds.has(record.id))
  }

  // New records use message-db as their only content and asset source.
  const trackerStates = new Map<string, RepeatTrackerState>()
  const guildTasks = new Map<string, Promise<void>>()
  const skippedCaptureWarnings = new Set<string>()
  let nextRecordId: number | undefined
  let allocationTask: Promise<void> = Promise.resolve()

  const enqueueGuild = async <T>(gid: string, task: () => Promise<T>): Promise<T> => {
    const previous = guildTasks.get(gid) ?? Promise.resolve()
    const current = previous.catch(() => {}).then(task)
    const tail = current.then(() => {}, () => {})
    guildTasks.set(gid, tail)
    try {
      return await current
    }
    finally {
      if (guildTasks.get(gid) === tail) guildTasks.delete(gid)
    }
  }

  const allocateRecordId = async () => {
    let allocated: number
    const task = allocationTask.then(async () => {
      if (nextRecordId === undefined) {
        const [legacy, current] = await Promise.all([
          ctx.database.select('w-repeat-record')
            .orderBy('id', 'desc').limit(1).project(['id']).execute(),
          ctx.database.select('w-repeat-record-v2')
            .orderBy('id', 'desc').limit(1).project(['id']).execute(),
        ])
        nextRecordId = Math.max(legacy[0]?.id ?? 0, current[0]?.id ?? 0)
      }
      allocated = ++ nextRecordId
    })
    allocationTask = task.catch(() => {})
    await task
    return allocated
  }

  const getReferencedKeys = (candidate: RepeatCandidate) => [...new Set([
    ...candidate.messages.map(message => message.key),
    candidate.interrupter?.key,
    ...candidate.suspensions.flatMap(suspension => [
      suspension.suspendMessageKey,
      suspension.resumeMessageKey,
    ]),
  ].filter((key): key is string => !! key))]

  const persistCandidate = async (candidate: RepeatCandidate): Promise<number> => {
    if (candidate.persistedId !== undefined) {
      const id = candidate.persistedId
      // A resumed candidate only grows: retain the superset first, then extend
      // its ordered rows, and publish the updated metadata last. This keeps the
      // previous valid record readable if the process exits between writes.
      await ctx.messageDb.retainMessages(`w-repeat:${id}`, getReferencedKeys(candidate))
      await ctx.database.upsert('w-repeat-message-v2', candidate.messages.map((message, index) => ({
        recordId: id,
        index,
        messageKey: message.key,
        platform: message.platform,
        userId: message.userId,
      })))
      await ctx.database.upsert('w-repeat-record-v2', [{
        id,
        platform: candidate.messages[0].platform,
        guildId: candidate.messages[0].guildId,
        starterMessageKey: candidate.messages[0].key,
        interrupterMessageKey: candidate.interrupter?.key,
        startTime: candidate.startTime,
        endTime: candidate.endTime,
        suspensions: candidate.suspensions,
      }])
      return id
    }

    while (true) {
      const id = await allocateRecordId()
      const owner = `w-repeat:${id}`
      try {
        await ctx.database.create('w-repeat-record-v2', {
          id,
          platform: candidate.messages[0].platform,
          guildId: candidate.messages[0].guildId,
          starterMessageKey: candidate.messages[0].key,
          interrupterMessageKey: candidate.interrupter?.key,
          startTime: candidate.startTime,
          endTime: candidate.endTime,
          suspensions: candidate.suspensions,
        })
      }
      catch (error) {
        if ((error as any)?.code === 'duplicate-entry') {
          nextRecordId = undefined
          continue
        }
        throw error
      }

      try {
        await ctx.database.upsert('w-repeat-message-v2', candidate.messages.map((message, index) => ({
          recordId: id,
          index,
          messageKey: message.key,
          platform: message.platform,
          userId: message.userId,
        })))
        await ctx.messageDb.retainMessages(owner, getReferencedKeys(candidate))
        return id
      }
      catch (error) {
        await Promise.allSettled([
          ctx.database.remove('w-repeat-message-v2', { recordId: id }),
          ctx.database.remove('w-repeat-record-v2', { id }),
          ctx.messageDb.releaseMessages(owner),
        ])
        throw error
      }
    }
  }

  const removePersistedCandidate = async (id: number) => {
    await ctx.database.remove('w-repeat-message-v2', { recordId: id })
    await ctx.database.remove('w-repeat-record-v2', { id })
    await ctx.messageDb.releaseMessages(`w-repeat:${id}`)
  }

  const toTrackedMessage = (message: SavedMessage): TrackedMessage => ({
    key: message.key,
    platform: message.platform,
    guildId: message.guildId,
    userId: message.userId,
    content: message.content,
    timestamp: message.timestamp,
    quoteId: message.quoteId,
  })

  const getMessageUid = (message: TrackedMessage) => `${message.platform}:${message.userId}`

  const updateV2RepeatStats = async (candidate: RepeatCandidate, uid: string) => {
    const senders = candidate.messages.map(getMessageUid)
    await Promise.all([
      ctx.database.upsert('w-repeat-user', row => [{
        uid,
        repeatCount: $inc(row.repeatCount),
        repeatTime: senders.slice(0, -1).includes(uid)
          ? undefined
          : $inc(row.repeatTime),
      }]),
      ctx.database.upsert('w-repeat-user', row => [{
        uid: senders[0],
        beRepeatedCount: $inc(row.beRepeatedCount),
        beRepeatedTime: senders.length === 2
          ? $inc(row.beRepeatedTime)
          : undefined,
      }]),
    ])
  }

  const updateV2WordStats = async (candidate: RepeatCandidate) => {
    const words = await ctx.messageDb.ensureMessageWords([candidate.messages[0].key])
    const counts = new Map<string, { word: string, tag: string, count: number }>()
    for (const { word, tag } of words) {
      const key = JSON.stringify([word, tag])
      const item = counts.get(key) ?? { word, tag, count: 0 }
      item.count ++
      counts.set(key, item)
    }
    if (! counts.size) return
    const gid = `${candidate.messages[0].platform}:${candidate.messages[0].guildId}`
    await ctx.database.upsert('w-repeat-word', row => [...counts.values()].map(item => ({
      gid,
      word: item.word,
      tag: item.tag,
      count: $.add(row.count, item.count),
    })))
  }

  ctx.middleware(async (session, next) => {
    if (! session.guildId) return next()
    const { content: originalContent, gid, uid } = session
    const settings = getGuildSettings(gid)
    if (gid in config.guildSettings) {
      if (! config.guildSettings[gid]) return next()
    }
    else if (! settings.doWrite) return next()

    if (settings.repeatBlacklist.some(pattern => {
      try {
        return new RegExp(pattern).test(originalContent)
      }
      catch (error) {
        ctx.logger.warn('invalid repeat blacklist pattern %o: %s', pattern, error)
        return false
      }
    })) return next()

    const reply = await enqueueGuild(gid, async () => {
      const saved = await ctx.messageDb.captureMessage(session)
      if (! saved) {
        if (! skippedCaptureWarnings.has(gid)) {
          skippedCaptureWarnings.add(gid)
          ctx.logger.warn('message-db skipped messages from %s; repeat tracking is disabled there', gid)
        }
        return
      }
      skippedCaptureWarnings.delete(gid)

      const state = trackerStates.get(gid) ?? createRepeatTrackerState()
      trackerStates.set(gid, state)
      const transition = transitionRepeat(state, toTrackedMessage(saved), {
        maxUnrelatedCount: settings.maxUnrelatedCount,
        repeatCount: settings.repeatCount,
      })

      if (transition.resumedRecordId !== undefined) {
        ctx.logger.debug('resumed repeat record %d in %s', transition.resumedRecordId, gid)
      }
      if (transition.stats) await updateV2RepeatStats(transition.stats, uid)
      if (transition.completed) {
        const isNewRecord = transition.completed.persistedId === undefined
        const id = await persistCandidate(transition.completed)
        if (isNewRecord && settings.enableSegmentation) {
          try {
            await updateV2WordStats(transition.completed)
          }
          catch (error) {
            // Word counts are a rebuildable cache and must not roll back a
            // successfully persisted repeat record.
            ctx.logger.warn('failed to update repeat word cache: %o', error)
          }
        }
        await ctx.database.upsert('w-repeat-user', row => [{
          uid,
          interruptTime: $inc(row.interruptTime),
        }])
        suspendPersistedCandidate(state, transition.completed, id)
      }

      if (! transition.shouldRepeat) return
      return h('as-forward', { level: 'never' }, [
        transition.current.quoteValid && transition.current.quoteId
          ? h.quote(transition.current.quoteId)
          : '',
        ...h.parse(transition.current.messages[0].content),
      ])
    })

    return reply ?? next()
  }, config.isPrepend)

  // 复读指令
  ctx.command('repeat', '群复读功能')

  ctx.command('repeat.user [user:user]', '查看用户复读统计')
    .action(async ({ session }, uid) => {
      const [ user ] = await ctx.database.get('w-repeat-user', { uid: uid || session.uid })
      if (! user) return `还没有复读统计`
      return dedent`
        复读条数：　　${user.repeatCount}
        复读次数：　　${user.repeatTime}
        被复读条数：　${user.beRepeatedCount}
        被复读次数：　${user.beRepeatedTime}
        打断复读次数：${user.interruptTime}
      `
    })

  ctx.command('repeat.stat', '查看群复读统计')
    .alias('repeat.s')
    .alias('repeat.guild')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）', {
      conflictsWith: { option: 'global', value: true }
    })
    .option('global', '-G 指定群（默认为本群）')
    .option('duration',
      '-d <duration:string> 指定时间范围。可以为 hour/day/week/month/all，或者用波浪号（~）分割的开始、结束时间',
      { fallback: 'day' }
    )
    .option('page', '-p <page:posint> 查看分页', { fallback: 1 })
    .option('list', '-l 显示复读记录列表', { fallback: true })
    .option('list', '-L 不显示复读记录列表', { value: false })
    .option('top', '-t <top:natural> 排行榜人数', { fallback: 1 })
    .option('filter', '-f <content:string> 根据查找复读记录', requireList())
    .option('image', '-i 查找包含图片的复读记录', requireList())
    .option('starter', '--us <user:user> 根据发起者查找（默认为自己）', requireList())
    .option('repeater', '--ur <user:user> 根据参与者查找（默认为自己）', requireList())
    .option('interrupter', '--ui <user:user> 根据打断者查找（默认为自己）', requireList())
    .option('jsfilter', '-j <code:string>', {
      authority: 4,
      conflictsWith: [ 'filter', { option: 'list', value: false } ]
    })
    .option('sort', '-s <sortby> 指定排序方式', { type: /^(count|tps|startTime)?(:(desc|asc))?$/, fallback: 'count' })
    .action(async ({ session, options }) => {
      const { global: isGlobal, jsfilter, top: topNum, duration } = options
      let { guild: gid } = options
      if (! session.guildId && ! options.guild && ! isGlobal) return '请在群内调用'
      if (! isGlobal) gid ||= session.gid

      const [ sortMethod = 'count', sortDirection = 'desc' ] = options.sort.split(':') as [
        'count' | 'tps' | 'startTime', Direction
      ]
      const isFiltered = Boolean(jsfilter || ([
        'filter', 'image', 'starter', 'repeater', 'interrupter'
      ] satisfies (keyof typeof options)[]).some(name => name in options))

      const v2Records = (await getV2Records(
        isGlobal ? undefined : gid,
        parseDuration(duration),
      )).filter(record =>
        (! options.filter || new RegExp(options.filter).test(record.content)) &&
        (! options.starter || record.senders[0] === options.starter) &&
        (! options.repeater || record.senders.includes(options.repeater)) &&
        (! options.interrupter || record.interrupter === options.interrupter) &&
        (! options.image || record.hasImage)
      )
      let recs = [
        ...v2Records.map(record => ({
          ...record,
          count: record.senders.length,
          tps: record.senders.length / (record.endTime - record.startTime) * 1000,
        })),
      ].sort((a, b) => {
        const difference = a[sortMethod] - b[sortMethod]
        return sortDirection === 'asc' ? difference : - difference
      })

      if (jsfilter) recs = recs.filter(eval(jsfilter))

      const topInterrupters = countAndSortBy(recs, rec => rec.interrupter)
      const topStarters = countAndSortBy(recs, rec => rec.senders[0])
      const topRepeaters = countAndSortBy(recs, rec => rec.senders)

      const memberDict = isGlobal ? null : await getMemberDict(session, gid.split(':')[1])

      const topText = (action: string, tops: [string, number][]) => dedent`
        ${action}最多的${topNum > 1 ? ` ${topNum} 名群友` : ''}是：${tops
          .slice(0, topNum)
          .map(([ uid, count ]) => `[${getMemberName(memberDict, uid)} * ${count}]`)
          .join(', ')
        }
      ` + (topNum >= 3 ? '\n' : '')
      const durationText = {
        'all': '',
        '~': '',
        'hour': '最近一小时',
        'day': '今日',
        'week': '本周',
        'month': '本月'
      } [duration] ?? `在 ${duration} `
      const { [sortMethod]: sortMethodText } = {
        'count': '复读次数',
        'tps': '每秒复读次数',
        'startTime': '开始时间'
      } satisfies Record<typeof sortMethod, string>
      const { [sortDirection]: sortDirectionText } = {
        'desc': '降序',
        'asc': '升序'
      } satisfies Record<Direction, string>

      const total = recs.length
      const groupText = options.global
        ? '所有群'
        : options.guild
          ? (await session.bot.getGuild(gid.split(':')[1])).name
          : '本群'
      const filterText = [
        options.starter && `由 ${ getMemberName(memberDict, options.starter) } 发起的`,
        options.repeater && `有 ${ getMemberName(memberDict, options.repeater) } 参与的`,
        options.interrupter && `被 ${ getMemberName(memberDict, options.interrupter) } 打断的`,
        options.image && '包含图片的',
        options.filter && `符合 /${options.filter}/ 的`,
        jsfilter && `符合 \`${jsfilter}\``
      ].filter(s => s).join('、')
      if (! total) return `${groupText}${durationText}还没有复读。在？为什么不复读？`

      const { displayPageSize: pageSize, displayLength } = config
      const pageNum = Math.ceil(total / pageSize)
      const pageId = options.page
      if (pageId < 1 || pageId > pageNum) return `页数必须为 1 到 ${pageNum} 间的整数。`

      const getListText = () => recs
        .slice((pageId - 1) * pageSize, pageId * pageSize)
        .map((rec, i) => {
          const content = ellipsis(unescapeMessage(rec, { allowImage: false }), displayLength)
          const times = ` * ${rec.senders.length}`
          const extra =
            sortMethod === 'tps' ? `, ${rec.tps.toFixed(2)}/s` :
            ''
          return `${i + 1}. [${content}${times}${extra}] # ${rec.id}`
        })
        .join('\n')

      const text = (options.list
        ? dedent`
          ${groupText}${durationText}共有 ${recs.length} 次${filterText}复读
          按${sortMethodText}${sortDirectionText}排序依次为：（第 ${pageId} / ${pageNum} 页）
          ${getListText()}
        ` + '\n\n'
        : ''
      ) + (! isFiltered && topNum > 0
        ? dedent`
          ${topText('参与复读', topRepeaters)}
          ${topText('发起复读', topStarters)}
          ${topText('打断复读', topInterrupters)}
        `
        : ''
      )

      return text
    })

  ctx.command('repeat.word', '查看群复读词频统计')
    .alias('repeat.w')

  ctx.command('repeat.word.word', '查看群内最常被复读的词')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）', {
      conflictsWith: { option: 'global', value: true }
    })
    .option('global', '-G 指定群（默认为本群）')
    .option('top', '-t <top:natural>', { fallback: 20 })
    .option('tag', '-T <tag:string>')
    .option('all', '-a 显示所有词（包括黑名单中的）')
    .action(async ({
      session: { guildId, gid },
      options: { guild: assignedGid, global: isGlobal, top: topCount, all, tag: tagStr }
    }) => {
      if (! guildId && ! assignedGid && ! isGlobal) return '请在群内调用'
      if (assignedGid) gid = assignedGid
      const settings = getGuildSettings(gid)

      const tags = tagStr ? tagStr.split(',') : null

      const words = await ctx.database
        .select('w-repeat-word')
        .where({
          gid: isGlobal ? {} : gid,
          word: all ? {} : { $not: { $in: settings.segmentationWordBlacklist } },
          tag: {
            $and: [
              all ? {} : { $not: { $in: settings.segmentationTagBlacklist } },
              tags ? { $in: tags } : {}
            ]
          }
        })
        .orderBy('count', 'desc')
        .limit(topCount)
        .execute()

      const filterText = tags
        ? `词性为 ${tags.join(' | ')} 的`
        : ''

      const wordsText = words
        .map(({ word, tag, count }, index) => `${index + 1}. ${word} [${tag}]: ${count}`)
        .join('\n')

      return `群内最经常被复读的${filterText} ${topCount} 个词是：\n\n${wordsText}`
    })

  ctx.command('repeat.word.tag', '查看群内最常被复读的词性')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）', {
      conflictsWith: { option: 'global', value: true }
    })
    .option('global', '-G 指定群（默认为本群）')
    .option('top', '-t <top:natural>', { fallback: 20 })
    .option('all', '-a 显示所有词（包括黑名单中的）')
    .action(async ({
      session: { guildId, gid },
      options: { guild: assignedGid, global: isGlobal, top: topCount, all }
    }) => {
      if (! guildId && ! assignedGid && ! isGlobal) return '请在群内调用'
      if (assignedGid) gid = assignedGid
      const settings = getGuildSettings(gid)

      const tags = await ctx.database
        .select('w-repeat-word')
        .where({
          gid: isGlobal ? {} : gid,
          tag: all ? {} : { $not: { $in: settings.segmentationTagBlacklist } }
        })
        .groupBy('tag', {
          count: row => $.sum(row.count)
        })
        .orderBy('count', 'desc')
        .limit(topCount)
        .execute()

      const tagsText = tags
        .map(({ tag, count }, index) => `${index + 1}. ${tag}: ${count}`)
        .join('\n')

      return `群内最经常被复读的 ${topCount} 个词性是：\n\n${tagsText}`
    })

  ctx.command('repeat.graph', '查看复读相关图表')
    .alias('repeat.g')

  ctx.command('repeat.graph.flow', '查看群复读流向图')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）')
    .option('minflow', '-m <minflow:string> 流量最小大小，低于该指的流向线条不显示，可用百分数表示最大流量的百分比', { fallback: '0' })
    .option('duration',
      '-d <duration:string> 指定时间范围。可以为 hour/day/week/month/all，或者用波浪号（~）分割的开始、结束时间',
      { fallback: 'day' }
    )
    .action(async ({ session, options }) => {
      if (! ctx.echarts) return '此指令需要 echarts 服务'

      if (! session.guildId && ! options.guild) return '请在群内调用'
      const gid = options.guild ?? session.gid
      const [, guildId ] = gid.split(':')

      const memberDict = await getMemberDict(session, guildId)

      const starterDict: Record<string, { name: string, count: number }> = {}
      const sendMat: Record<string, Record<string, { count: number }>> = {}
      const recs = await getV2Records(gid, parseDuration(options.duration))
      if (! recs.length) return '指定时间内还没有复读记录。'

      recs.forEach(rec => {
        const starter = rec.senders[0]
        void (starterDict[starter] ??= {
          name: getMemberName(memberDict, starter),
          count: 0
        }).count ++
        rec.senders.slice(1).forEach(sender => {
          ((sendMat[sender] ??= {})[starter] ??= { count: 0 }).count ++
        })
      })

      type GraphSeriesOption = echarts.RegisteredSeriesOption['graph']

      const users = [ ...new Set([
        ...Object.keys(starterDict),
        ...Object.keys(sendMat)
      ]) ].map(uid => ({
        uid,
        name: starterDict[uid]?.name ?? getMemberName(memberDict, uid),
        count: starterDict[uid]?.count ?? 0
      }))
      const userNum = users.length
      const maxRepeatedCount = Math.max(1, ...users.map(({ count }) => count))
      const nodes = users
        .map<GraphSeriesOption['data'][number]>(({ uid, name, count }, i) => ({
          name: uid,
          label: {
            show: true,
            formatter: name,
            color: '#000',
            borderColor: 'transparent',
            shadowColor: 'transparent',
            fontSize: 22
          },
          symbolSize: 30 + count / maxRepeatedCount * 120,
          category: String(i),
        }))

      const flows = Object
        .entries(sendMat)
        .flatMap(([ source, targetRow ]) => Object
          .entries(targetRow)
          .map(([ target, { count } ]) => ({
            source,
            target,
            count
          }))
        )
      const maxRepeatFlowSize = Math.max(...flows.map(({ count }) => count))

      const tryParseNumber = (s: string): number => {
        const n = Number(s)
        if (Number.isNaN(n)) throw new SessionError(`${s} 不是合法的数字`)
        return n
      }
      const minFlowSize = options.minflow.endsWith('%')
        ? maxRepeatFlowSize * .01 * tryParseNumber(options.minflow.slice(0, - 1))
        : tryParseNumber(options.minflow)
      const links = flows
        .filter(flow => flow.count >= minFlowSize)
        .map<GraphSeriesOption['links'][number]>(({ source, target, count }) => ({
          source,
          target,
          lineStyle: {
            width: count / maxRepeatFlowSize * 50,
            curveness: 0.2,
            type: 'solid',
            color: 'source'
          }
        }))

      const eh = ctx.echarts.createChart(800, 800, {
        series: {
          type: 'graph',
          width: 560,
          height: 560,
          layout: 'circular',
          label: {
            overflow: 'break',
            width: 100
          },
          circular: {
            rotateLabel: true
          },
          categories: Array
            .from({ length: userNum })
            .map((_, i) => ({ name: String(i) })),
          data: nodes,
          links
        },
        backgroundColor: '#fff'
      })

      return eh.export(3000)
    })

  ctx.command('repeat.graph.time', '查看群复读时段图')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）')
    .action(async ({ session, options }) => {
      if (! ctx.echarts) return '此指令需要 echarts 服务'

      if (! session.guildId && ! options.guild) return '请在群内调用'
      const gid = options.guild ?? session.gid

      // TODO: optimize
      const recs = await ctx.database.get('w-repeat-record-v2', {
        platform: splitWithLimit(gid, ':', 2)[0],
        guildId: splitWithLimit(gid, ':', 2)[1],
      })

      const timeMat: Record<string, Record<string, number>> = Object.fromEntries(
        Array.from({ length: 24 }).map((_, i) => [
          i,
          Object.fromEntries(Array.from({ length: 7 }).map((_, j) => [j, 0]))
        ])
      )

      recs.forEach(({ startTime }) => {
        const time = dayjs(startTime)
        const day = time.day()
        const hour = time.hour()
        timeMat[hour][day] ++
      })

      const data = Object
        .entries(timeMat)
        .flatMap(([ hour, dayRow ]) => Object
          .entries(dayRow)
          .map(([ day, count ]) => [ + hour, + day, count ])
        )

      const eh = ctx.echarts.createChart(24 * 30 + 100, 7 * 30 + 120, {
        xAxis: {
          type: 'category',
          data: Array.from({ length: 24 }).map((_, i) => `0${i}`.slice(-2)),
          splitArea: { show: true }
        },
        yAxis: {
          type: 'category',
          data: [ 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' ],
          splitArea: { show: true }
        },
        visualMap: {
          min: 0,
          max: Math.max(...data.map(it => it[2])),
          calculable: true,
          show: false
        },
        series: {
          type: 'heatmap',
          silent: true,
          label: { show: true },
          data
        },
        backgroundColor: '#fff'
      })

      return eh.export()
    })

  ctx.command('repeat.graph.top-calendar [month:string]', '查看群复读排行日历')
    .alias('repeat.graph.topc')
    .option('guild', '-g <guild:channel> 指定群（默认为本群）')
    .option('type', '-t <type> 排行榜类型，可以为 r(epeater) / (s)tarter / i(nterrupter) / a(ll)，默认为 all', {
      type: /^(r|repeater|s|starter|i|interrupter|a|all)$/,
      fallback: 'all'
    })
    .option('avatar', '-a 使用头像', { fallback: true })
    .option('avatar', '-A 不使用头像', { value: false })
    .action(async ({ session, options }, month) => {
      if (! ctx.echarts) return '此指令需要 echarts 服务'

      if (! session.guildId && ! options.guild) return '请在群内调用'
      const gid = options.guild ?? session.gid

      const date = month ? dayjs(month, 'YYYY-MM', true) : dayjs()
      if (! date.isValid()) return `${month} 不是合法的月份，月份格式应为 YYYY-MM`
      month = date.format('YYYY-MM')

      const days = getDaysOfMonth(date)
      const today = dayjs().format('YYYY-MM') === month
        ? dayjs().format('DD')
        : undefined

      type TopType = 'repeater' | 'starter' | 'interrupter'
      const topTypeText = {
        starter: '发起者',
        repeater: '参与者',
        interrupter: '打断者'
      } satisfies Record<TopType, string>

      const getCalendar = async (topType: TopType): Promise<{
        output: string,
        dataToUpsert: Update<RepeatDay>[]
      }> => {
        const idKey = `top${capitalize(topType)}Id` as const
        const countKey = `top${capitalize(topType)}Count` as const

        const dataExists = await ctx.database
          .select('w-repeat-calendar')
          .where({
            gid,
            month,
            ...(today ? { day: { $ne: today } } : {}),
            [idKey]: { $ne: null }
          })
          .project({
            month: row => row.month,
            day: row => row.day,
            id: row => row[idKey],
            count: row => row[countKey]
          })
          .execute()
        const missingDays = exclude(days, dataExists.map(rec => rec.day))

        const [ memberDict, [ dataCreated, dataToUpsert ] ] = await Promise.all([
          getMemberDict(session, gid.split(':')[1]),
          unzipPromise(Promise.all(missingDays.map(async day => {
            const date = dayjs(`${month}-${day}`)
            const start = + date.startOf('day')
            const end = + date.endOf('day')
            const recs = await getV2Records(gid, {
              startTime: { $gte: start, $lte: end },
            })
            if (! recs.length) return [
              { month, day, id: null, count: null },
              { gid, month, day }
            ]
            const candidatorDict: Record<string, number> = {}
            const inc = safeInc(candidatorDict)
            recs.forEach(rec => {
              if (topType === 'repeater') rec.senders.forEach(inc)
              else if (topType === 'starter') inc(rec.senders[0])
              else inc(rec.interrupter)
            })
            const [ [ id, count ] ] = Object
              .entries(candidatorDict)
              .sort(([, count1 ], [, count2 ]) => count2 - count1)
            return [
              { month, day, id, count },
              { gid, month, day, [idKey]: id, [countKey]: count }
            ] as const
          })))
        ])

        const data = [ ...dataExists, ...dataCreated ]
        const maxCount = Math.max(0, ...data.map(({ count }) => count ?? 0))

        const CELL_SIZE = 80

        const eh = ctx.echarts.createChart(700, 500, {
          backgroundColor: '#fff',
          calendar: {
            orient: 'vertical',
            yearLabel: {
              show: false
            },
            monthLabel: {
              nameMap: 'cn',
              margin: 20,
              fontSize: 20,
              fontWeight: 600
            },
            dayLabel: {
              nameMap: [ 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat' ],
              firstDay: 1,
              margin: 20,
              fontSize: 17
            },
            cellSize: CELL_SIZE,
            range: month
          },
          visualMap: {
            min: 0,
            max: maxCount,
            calculable: true,
            show: false
          },
          series: {
            type: 'custom',
            coordinateSystem: 'calendar',
            renderItem: (_, api) => {
              const day = api.value('day') as string
              const id = api.value('id') as string
              const count = api.value('count') as number
              const [ x, y ] = api.coord(`${month}-${day}`)

              type CustomSeriesOption = echarts.RegisteredSeriesOption['custom']
              type CustomGroupOption = ReturnType<CustomSeriesOption['renderItem']> & { type: 'group' }

              const children: CustomGroupOption['children'] = [
                options.avatar && id ? {
                  type: 'image',
                  style: {
                    x: x - CELL_SIZE / 2 + 4,
                    y: y - CELL_SIZE / 2 + 4,
                    width: CELL_SIZE - 8,
                    height: CELL_SIZE - 8,
                    image: memberDict[id]?.user?.avatar,
                    shadowColor: '#73b9bc',
                    shadowBlur: day === today ? 4 : 0,
                  }
                } : null,
                id ? {
                  type: 'text',
                  style: {
                    text: String(count),
                    x,
                    y: y + CELL_SIZE / 2 - 20 - 2,
                    align: 'center',
                    fill: '#000',
                    stroke: '#fff',
                    lineWidth: 2,
                    textFont: api.font({ fontSize: 16, fontWeight: 'bold' })
                  }
                } : null,
                {
                  type: 'text',
                  style: {
                    x: x - CELL_SIZE / 2 + 2,
                    y: y - CELL_SIZE / 2 + 2,
                    text: day,
                    fill: '#000',
                    textFont: api.font({ fontSize: 16 })
                  }
                }
              ]

              return {
                type: 'group',
                children: children.filter(child => child !== null)
              }
            },
            dimensions: [
              undefined,
              { name: 'day', type: 'ordinal' },
              { name: 'id', type: 'ordinal' },
              { name: 'count', type: 'int' }
            ],
            data: data.map(({ day, id, count }) =>
              [ undefined, day, id, count ]
            )
          }
        })

        const topText = topTypeText[topType]

        return {
          output: `本月群${topText}排行榜：\n` + await eh.export(10000),
          dataToUpsert
        }
      }

      // TODO: Optimize
      if (options.type === 'a' || options.type === 'all') {
        const outputs: string[] = []
        for (const topType of [ 'repeater', 'starter', 'interrupter' ] satisfies TopType[]) {
          const { output, dataToUpsert } = await getCalendar(topType)
          await ctx.database.upsert('w-repeat-calendar', dataToUpsert)
          outputs.push(output)
        }
        const output = h('message', { forward: true }, outputs.map(output => h('message', h.parse(output))))
        return output
      }

      const topType = ({
        repeater: 'repeater',
        r: 'repeater',
        starter: 'starter',
        s: 'starter',
        interrupter: 'interrupter',
        i: 'interrupter'
      } as const)[options.type]

      const { output, dataToUpsert } = await getCalendar(topType)
      await ctx.database.upsert('w-repeat-calendar', dataToUpsert)
      return output
    })

  ctx.command('repeat.record <id:posint>', '查看某次复读详情')
    .alias('repeat.r')
    .option('all-senders', '-a 显示所有参与者')
    .option('suspension', '-s 显示挂起详情')
    .option('delete', '-d 删除此复读详情', { authority: 4 })
    .option('ocr', '-o 识别图片中文字')
    .option('segmentation', '--seg 对复读消息分词')
    .action(async ({ session, options }, id) => {
      const [v2Record] = await ctx.database.get('w-repeat-record-v2', { id })
      const rec = v2Record && (await hydrateV2Records([v2Record]))[0]
      if (! rec) return `未找到复读 #${id}。`

      const guildId = rec.gid.split(':')[1]
      const memberDict = await getMemberDict(session, guildId)
      const guild = await session.bot.getGuild(guildId)

      let content: string

      if (options.delete) {
        if (rec.storageVersion === 2) {
          await removePersistedCandidate(id)
        }
        content = '[已删除]'
      }
      else {
        content = unescapeMessage(rec)
        if (options.ocr) {
          if (rec.storageVersion === 2) return 'message-db 记录暂不支持写入 OCR 结果'
          if (tesseractWorker) await updateImageText(rec)
          else return 'Tesseract 未加载，无法识别图片中文字'
        }
        if (options.segmentation) {
          if (rec.storageVersion === 2) {
            rec.words = await ctx.messageDb.getWordsByMessageKeys([v2Record.starterMessageKey])
          }
          else {
          if (jieba) await updateWords(rec)
          else return 'Jieba 未加载，无法分词'
          }
        }
      }

      const sendersText = options['all-senders']
        ? rec.senders.map(uid => getMemberName(memberDict, uid)).join('，')
        : `${rec.senders.length} 个`

      const suspensionText = rec.suspensions?.length
        ? options.suspension
          ? '\n' + rec.suspensions
            .map(({ suspendTime, resumeTime }, i) =>
              `${i + 1}. 挂起时间：${timeText(suspendTime)}，恢复时间：${timeText(resumeTime)}`
            )
            .join('\n')
          : `挂起并恢复了 ${rec.suspensions.length} 次`
        : '无'

      return dedent`
        复读 #${id} 详情
        群：${guild.name}${guildId === session.guildId ? '（本群）' : ''}
        发起者：${getMemberName(memberDict, rec.senders[0])}
        发起时间：${timeText(rec.startTime)}
        打断者：${getMemberName(memberDict, rec.interrupter)}
        参与者：${sendersText}
        打断时间：${timeText(rec.endTime)}
        挂起情况：${suspensionText}
        内容：${options.delete ? '[已删除]' : content}`
        + ((rec.images?.length ?? 0) || options.ocr
          ? `\n图片识别结果：${options.ocr ? '[新识别]' : ''}\n${(rec.images ?? [])
            .map(({ text }, i) => `${i + 1}. ${text.trim() || '[未识别到文字]'}`)
            .join('\n')
          }`
          : ''
        )
        + (rec.words
          ? `\n分词结果：${rec.words.map(({ word, tag }) => `${word} [${tag}]`).join(', ')}`
          : ''
        )
    })

  ctx.command('repeat.debug', '复读调试', { hidden: true })

  ctx.command('repeat.debug.eval <code:text>', '在本插件作用域中运行 JavaScript', { authority: 4 })
    .action(async (_, code) => {
      try {
        return JSON.stringify(await eval(code), null, 2)
      }
      catch (error) {
        return String(error)
      }
    })

  ctx.command('repeat.debug.runtime', '获取当前复读运行时', { authority: 2 })
    .action(({ session }) => '当前运行时：\n'
      + h.escape(JSON.stringify(trackerStates.get(session.gid), null, 2))
    )

  ctx.command('repeat.debug.runtime.clear', '清除复读运行时', { authority: 2 })
    .option('all', '-a 清除所有')
    .action(async ({ session: { gid }, options: { all } }) => {
      const gids = all ? [...trackerStates.keys()] : [gid]
      gids.forEach(gid => trackerStates.delete(gid))

      return `清除了 ${gids.length} 个运行时`
    })

  ctx.command('repeat.debug.runtime.list', '获取复读运行时列表', { authority: 2 })
    .action(() => '运行时列表：' + [...trackerStates.keys()].join(', '))

  ctx.command('repeat.admin', '复读管理')
    .alias('repeat.a')

  let migrationTask: {
    controller: AbortController
    promise: Promise<void>
  } | undefined
  let migrationError: string | undefined

  const describeMigration = async () => {
    const [state] = await ctx.database.get('w-repeat-migration', REPEAT_MESSAGE_MIGRATION_ID)
    if (! state) return '复读记录迁移尚未开始。'
    const status = migrationTask
      ? '运行中'
      : migrationError
        ? `失败（${migrationError}）`
        : state.completedAt
          ? `已完成（${dayjs(state.completedAt).format('YYYY-MM-DD HH:mm:ss')}）`
          : '已暂停'
    return dedent`
      复读记录迁移：${status}
      进度：${state.processed}/${state.total}
      成功：${state.migrated}
      未匹配：${state.unmatched}
      有歧义：${state.ambiguous}
      游标：${state.cursor ?? '无'}
    `
  }

  ctx.command('repeat.admin.migrate-message-db [action:string]', '管理 message-db 复读记录迁移', { authority: 4 })
    .option('batch', '-b <batch:posint> 每批处理的记录数', { fallback: 100 })
    .action(async ({ options }, action = 'status') => {
      if (action === 'status') return describeMigration()
      if (action === 'stop') {
        if (! migrationTask) return '当前没有正在运行的复读记录迁移。'
        const task = migrationTask
        task.controller.abort()
        await task.promise
        return describeMigration()
      }
      if (action !== 'start') return '可用操作：start、stop、status。'
      if (migrationTask) return '复读记录迁移已经在运行。'

      const controller = new AbortController()
      migrationError = undefined
      let lastLogged = 0
      const promise = migrateRepeatRecords(ctx, {
        batchSize: options.batch,
        signal: controller.signal,
        onProgress: ({ state }) => {
          if (state.processed - lastLogged < options.batch && ! state.completedAt) return
          lastLogged = state.processed
          ctx.logger.info(
            'repeat migration: %d/%d processed, %d migrated, %d unmatched, %d ambiguous',
            state.processed, state.total, state.migrated, state.unmatched, state.ambiguous,
          )
        },
      }).then((state) => {
        ctx.logger.info(
          'repeat migration completed: %d migrated, %d unmatched, %d ambiguous',
          state.migrated, state.unmatched, state.ambiguous,
        )
      }).catch((error) => {
        if (error?.name === 'AbortError') {
          ctx.logger.info('repeat migration stopped')
          return
        }
        migrationError = error instanceof Error ? error.message : String(error)
        ctx.logger.error('repeat migration failed: %o', error)
      }).finally(() => {
        if (migrationTask?.controller === controller) migrationTask = undefined
      })
      migrationTask = { controller, promise }
      return '复读记录迁移已在后台启动；使用 repeat.admin.migrate-message-db status 查看进度。'
    })

  ctx.command('repeat.admin.settings [key:string] [value:string]', '管理群复读设置')
    .action(async ({ session, options }, key, value) => {
      const [ member, user ] = await Promise.all([
        session.bot.getGuildMember(session.guildId, session.userId),
        session.observeUser([ 'authority' ]),
      ])
      const isGuildAdmin = member.roles.some(role =>
        [ role.id, role.name ].some(value => value === 'admin' || value === 'owner')
      )
      if (user.authority < 3 && ! isGuildAdmin) {
        return '只有群主、管理员或 Koishi 管理员（权限等级 ≥ 3）可以管理群复读设置。'
      }

      // 群设置必须使用独立对象；直接复用全局设置会令群设置修改污染全局配置。
      const settings = config.guildSettings[session.gid] ??= {
        ...config.globalSettings,
        repeatBlacklist: [ ...config.globalSettings.repeatBlacklist ],
        segmentationWordBlacklist: [ ...config.globalSettings.segmentationWordBlacklist ],
        segmentationTagBlacklist: [ ...config.globalSettings.segmentationTagBlacklist ]
      }

      const displaySettingItem = (symbol: string, desc) => (key: string) => (
        `${key}${desc ? ` /* ${RepeatSettings.dict[key].meta.description} */` : ''}${symbol}${JSON.stringify(settings[key])}`
      )

      if (! key && ! value) {
        return (
          '群复读设置：{\n' +
          Object
            .keys(RepeatSettings.dict)
            .map(key => '  ' + displaySettingItem(': ', true)(key))
            .join('\n') +
          '\n}'
        )
      }

      if (! (key in RepeatSettings.dict)) {
        return `未知设置项 '${key}'`
      }

      if (! value) {
        return `群复读设置：${displaySettingItem(' == ', false)(key)}`
      }

      try {
        const validated = RepeatSettings.dict[key](JSON.parse(value))
        settings[key] = validated
        ctx.scope.update(config)
        return `已修改群复读设置：${displaySettingItem(' = ', false)(key)}`
      }
      catch {
        return `无法解析设置值 '${value}'`
      }
    })

  ctx.command('repeat.admin.regen-user-table', '重建复读用户表', { authority: 4 })
    .action(async ({ session }) => {
      await session.send('正在根据复读记录重建用户数据表……')
      await ctx.database.remove('w-repeat-user', {})
      const recs = [
        ...(await filterUnmigratedLegacy(await ctx.database.get('w-repeat-record', {}))).map(asUnifiedV1),
        ...await getV2Records(),
      ]
      const users: Record<string, RepeatUser> = {}
      const getUser = (uid: string): RepeatUser => users[uid] ??= {
        uid,
        repeatTime: 0,
        repeatCount: 0,
        beRepeatedTime: 0,
        beRepeatedCount: 0,
        interruptTime: 0
      }
      recs.forEach(rec => {
        const starter = getUser(rec.senders[0])
        starter.beRepeatedTime ++
        starter.beRepeatedCount += rec.senders.length - 1

        const counted: Record<string, boolean> = {}
        rec.senders.slice(1).forEach(uid => {
          const user = getUser(uid)
          user.repeatCount ++
          if (! counted[uid]) {
            counted[uid] = true
            user.repeatTime ++
          }
        })

        if (rec.interrupter) getUser(rec.interrupter).interruptTime ++
      })
      const writeResult = await ctx.database.upsert('w-repeat-user', () => Object.values(users))
      return `已重建 ${writeResult.inserted} 条用户数据`
    })

  ctx.command('repeat.admin.ocr-all', '识别所有消息图片', { authority: 4 })
    .action(({ session }) => profile(async () => {
      if (! tesseractWorker) return 'Tesseract 未加载，无法识别图片中文字'
      await session.send('开始查询数据库……')
      const recs = await filterUnmigratedLegacy(
        await ctx.database.get('w-repeat-record', row => $.gt($.length(row.images), 0)),
      )
      const imageCount = recs.reduce(
        (count, rec) => count + (rec.images ?? []).filter(x => x !== null).length,
        0
      )
      await session.send(`正在识别 ${recs.length} 条复读记录中的 ${imageCount} 张图片……`)
      await Promise.all(recs.map(updateImageText))
    }))

  ctx.command('repeat.admin.segmentation-all', '对所有消息分词', { authority: 4 })
    .action(({ session }) => profile(async () => {
      if (! jieba) return 'Jieba 未加载，无法分词'
      await session.send('正在清空分词表……')
      await ctx.database.remove('w-repeat-word', {})
      await session.send('开始查询数据库…… ')
      const legacyRecs = await filterUnmigratedLegacy(await ctx.database.get('w-repeat-record', {}))
      const v2Rows = await ctx.database.get('w-repeat-record-v2', {})
      for (let index = 0; index < v2Rows.length; index += 500) {
        await ctx.messageDb.ensureMessageWords(
          v2Rows.slice(index, index + 500).map(record => record.starterMessageKey),
        )
      }
      const v2Recs = await hydrateV2Records(v2Rows)
      await session.send(`正在对 ${legacyRecs.length + v2Recs.length} 条复读记录分词……`)
      const wordDict: Record<string, number> = {}
      const incWord = safeInc(wordDict)
      await Promise.all(legacyRecs.map(async rec => {
        const words = await updateWords(rec)
        words.forEach(({ tag, word }) => incWord(`${rec.gid}#${tag}#${word}`))
      }))
      v2Recs.forEach(rec => rec.words?.forEach(({ tag, word }) =>
        incWord(`${rec.gid}#${tag}#${word}`)))
      await session.send('正在写入分词表……')
      const words = Object.entries(wordDict).map(([ gidTagWord, count ]) => {
        const [ gid, tag, word ] = splitWithLimit(gidTagWord, '#', 3)
        return { gid, word, tag, count }
      })
      await ctx.database.upsert('w-repeat-word', words)
    }))

  ctx.command('repeat.admin.migrate-guild <from:channel> <to:channel>', '迁移群复读记录', { authority: 4 })
    .action(async (_, from, to) => {
      const [fromPlatform, fromGuildId] = splitWithLimit(from, ':', 2)
      const [toPlatform, toGuildId] = splitWithLimit(to, ':', 2)
      const [legacy, v2] = await Promise.all([
        ctx.database.set('w-repeat-record', { gid: from }, { gid: to }),
        ctx.database.set('w-repeat-record-v2', {
          platform: fromPlatform,
          guildId: fromGuildId,
        }, {
          platform: toPlatform,
          guildId: toGuildId,
        }),
      ])
      return `成功从 ${from} 迁移了 ${legacy.modified + v2.modified} 条复读记录到 ${to}。`
    })

  // 回收副作用
  ctx.on('dispose', () => {
    migrationTask?.controller.abort()
    // 终止 tesseract Worker
    tesseractWorker?.terminate()
  })
}
