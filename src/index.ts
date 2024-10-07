import {
    h, z,
    Argv, Context, Session, SessionError,
    $, Direction, Row, Query,
    Awaitable,
    Fragment
} from 'koishi'
import { type GuildMember } from '@satorijs/protocol'

import {} from '@koishijs/plugin-help'
import {} from 'koishi-plugin-w-option-conflict'
import {} from 'koishi-plugin-w-echarts'
import {} from 'koishi-plugin-w-tesseract'

import dedent from 'dedent'
import dayjs from 'dayjs'
import format from 'pretty-format'

export const name = 'w-repeat'

export const inject = {
    required: [ 'database' ],
    optional: [ 'echarts', 'tesseract' ]
}

export interface Config {
    repeatCount: number
    maxUnrelatedCount: number
    displayLength: number
    imageTextDisplayLength: number
    displayPageSize: number
    repeatBlacklist: string[]
    doProcessImage: boolean
    doOcr: boolean
    ocrLangs: string[]
    doWrite: boolean
}

export const Config: z<Config> = z.object({
    repeatCount: z.natural().default(0).description('机器人复读需要的次数，0 为不复读'),
    maxUnrelatedCount: z.natural().default(5).description('恢复挂起的复读前允许的最大无关消息条数，0 为禁用挂起'),
    displayLength: z.natural().default(25).description('复读消息最大长度，超过则显示为省略号'),
    imageTextDisplayLength: z.natural().default(15).description('图片文字预览最大长度，超过则显示为省略号'),
    displayPageSize: z.natural().min(5).default(10).description('复读消息分页大小，至少为 5'),
    repeatBlacklist: z.array(z.string()).description('复读内容黑名单'),
    doProcessImage: z.boolean().default(false).description('是否处理图片（会使用较多数据库空间）'),
    doOcr: z.boolean().default(true).description('是否自动识别复读消息图片中文字'),
    ocrLangs: z.array(z.string()).default([ 'chi_sim', 'eng' ]).description('识别图片中文字时尝试的语言，参考 ' +
        '<https://tesseract-ocr.github.io/tessdoc/Data-Files#data-files-for-version-400-november-29-2016>'
    ),
    doWrite: z.boolean().default(true).description('是否向数据库写入复读数据')
})

declare module 'koishi' {
    interface Tables {
        'w-repeat-record': RepeatRecord         // 复读记录表
        'w-repeat-user': RepeatUser             // 复读用户表
    }
}

export interface RepeatImage {
    b64: string
    text: string
}

export interface RepeatMessage {
    content?: string
    images?: RepeatImage[]
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

export async function apply(ctx: Context, config: Config) {
    // 扩展数据库模型
    ctx.model.extend('w-repeat-record', {
        id: 'unsigned',
        gid: 'string',
        content: 'string',
        senders: 'array',
        startTime: 'unsigned',
        endTime: 'unsigned',
        interrupter: 'string',
        images: 'array',
        suspensions: 'array'
    }, { autoInc: true })

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

    // 初始化 tesseract Worker

    let tesseractWorker = undefined
    const initTesseract = async () => {
        if (! config.doOcr) return
        const { ocrLangs } = config 
        await ctx.tesseract.installNecessaryLangs(ocrLangs)
        tesseractWorker = await ctx.tesseract.createWorker(ocrLangs)
    }
    await initTesseract()

    // 工具函数
    // String
    const ellipsis = (input: string, maxLength: number): string => {
        const els = h.parse(input)
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

    const timeText = (time: number) => dayjs(time).format('YYYY/MM/DD HH:mm:ss')

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

    // Dict
    const pick = <T extends {}, K extends keyof T>(x: T, keys: K[]): Pick<T, K> =>
        Object.fromEntries(Object.entries(x).filter(([ k ]) => keys.includes(k as any))) as any

    const omit = <T extends {}, K extends keyof T>(x: T, keys: K[]): Omit<T, K> =>
        Object.fromEntries(Object.entries(x).filter(([ k ]) => ! keys.includes(k as any))) as any

    const pickOr = <T extends {}, K extends keyof T>(x: T, keys: K[]): [ Pick<T, K>, Omit<T, K> ] => {
        const xPick = {} as Pick<T, K>
        const xOmit = {} as Omit<T, K>
        Object.keys(x).forEach(k => (keys.includes(k as any) ? xPick : xOmit)[k] = x[k])
        return [ xPick, xOmit ]
    }

    // Adapter
    const getMemberDict = async (session: Session, guildId: string) => {
        const { data: memberList } = await session.bot.getGuildMemberList(guildId)
        return memberList.reduce<Record<string, GuildMember>>((dict, member) => {
            dict[session.platform + ':' + member.user.id] = member
            return dict
        }, {})
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

    // Database
    const getReserveProjection = <S>(fields: (keyof S)[]): {
        [K in keyof S]: (row: Row<S>) => Row<S>[K]
    } => Object.fromEntries(fields.map(name => [ name, (row: Row<S>) => row[name] ])) as any

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

    // Stream
    const streamToBuffer = async (stream: ReadableStream<Uint8Array>): Promise<Buffer> => {
        const buffers: Uint8Array[] = []
        for await (const data of stream) buffers.push(data)
        return Buffer.concat(buffers)
    }

    // Repeat
    const isSameImages = (images1: RepeatImage[], images2: RepeatImage[]): boolean =>
            ! new Set([ ...images1, ...images2 ]).has(null)
        &&  images1.length === images2.length
        &&  images1.every((b1, i) => b1.b64 === images2[i].b64)

    const isSameMessage = (message1: RepeatMessage, message2: RepeatMessage) =>
            message1 && message2
        &&  message1.content === message2.content
        &&  isSameImages(message1.images ?? [], message2.images ?? [])

    const updateImageText = async (rec: RepeatRecord | RepeatQueuedRecord) => {
        const { images } = rec

        await Promise.all(images.filter(x => x !== null).map(async ({ b64 }, i) => {
            if (! b64) console.log(images)
            const res = await tesseractWorker.recognize(Buffer.from(b64, 'base64'))
            const { text } = res.data
            images[i].text = text
        }))

        if ('id' in rec) await ctx.database.set('w-repeat-record', { id: rec.id }, { images })
    }

    const createCurrentRec = (gid: string): RepeatQueuedRecord => ({
        gid,
        content: undefined,
        images: undefined,
        senders: undefined,
        startTime: undefined,
        endTime: undefined,
        interrupter: undefined,
        suspensions: [],
        unrelatedCount: 0
    })

    const unescapeMessage = (message: RepeatMessage, { allowImage = true }: { allowImage?: boolean } = {}): string => {
        let imageIdx = 0
        return message.content.replace(
            /@__KOISHI_IMG__@/g,
            () => {
                const image = message.images[imageIdx ++]
                return allowImage
                    ? h.img('data:image/png;base64,' + image.b64).toString()
                    : `[图片${ image.text ? ': ' + image.text : '' }]`
            }
        )
    }

    // 复读中间件
    const runtimes: Record<string, RepeatRuntime> = {}
    ctx.middleware(async (session, next) => {
        // 配置为不写入则跳过
        if (!config.doWrite) return next()

        // 只处理群内消息
        const { content: originalContent, gid, uid } = session
        if (!session.guildId) return next()

        // 过滤内容黑名单
        if (config.repeatBlacklist.some(re => new RegExp(re).test(originalContent)))
            return next()

        // 解析消息，处理图片
        const imageSrcs: string[] = []
        const content = h
            .parse(originalContent)
            .map(el => {
                if (config.doProcessImage && el.type === 'img') {
                    const src = el.attrs.src as string
                    imageSrcs.push(src)
                    return '@__KOISHI_IMG__@'
                }
                return el.toString()
            })
            .join('')
        const images: RepeatImage[] = await Promise.all(imageSrcs.map(async src => {
            try {
                const res = await fetch(src)
                const buffer = await streamToBuffer(res.body)
                return {
                    b64: buffer.toString('base64'),
                    text: ''
                }
            }
            catch (err) {
                ctx.logger.error('Failed download image <%s>, %o', src, err)
                return null
            }
        }))

        // 定义当前消息：内容和图片
        const thisMessage: RepeatMessage = { content, images }

        // 获取本群复读运行时，若无则创建
        const runtime = runtimes[gid] ??= {
            currentRec: undefined,
            queuedRecs: [],
            suspendedRecs: []
        }
        let { currentRec } = runtime

        // 判断当前消息是否为复读（即和当前复读内容相同）
        const isRepeating = isSameMessage(thisMessage, currentRec)

        // 处理某类复读
        const procRecs = async <K extends 'suspendedRecs' | 'queuedRecs'>(
            recsName: K,
            onSame: (rec: RepeatRuntime[K][number]) => Promise<void>
        ) => {
            await Promise.all(runtime[recsName].map(async (rec: RepeatRuntime[K][number]) => {
                // 当前消息与挂起复读内容相同
                if (isSameMessage(thisMessage, rec)) {
                    // 讲当前用户添加到复读发送者（暂不区分挂起状态、未激活状态下的发送者）
                    rec.senders.push(uid)
                    // 重置无关消息计数器
                    rec.unrelatedCount = 0
                    // 调用相同消息处理函数
                    await onSame(rec)
                }
                else {
                    // 增加无关消息计数器
                    const count = ++ rec.unrelatedCount
                    // 如果无关消息多于阈值，将该复读标记为删除
                    if (count > config.maxUnrelatedCount) rec.deleted = true
                }
            }))
            // 清理标记删除的复读
            runtime[recsName] = runtime[recsName].filter(rec => ! rec.deleted) as RepeatRuntime[K]
        }

        let isNewRec = true 

        // 处理挂起的复读
        await procRecs('suspendedRecs', async rec => {
            // 如果当前消息构成复读，则恢复挂起的复读
            if (isRepeating) {
                // 将挂起的复读记录分离为挂起信息和恢复的复读
                const [ suspension, resumed ] = pickOr(rec, [ 'suspendTime', 'resumeTime' ])
                // 添加挂起信息到恢复的复读中
                resumed.suspensions.push({
                    ...pick(suspension, [ 'suspendTime' ]),
                    resumeTime: Date.now()
                })
                // 将挂起的复读移出复读记录表，并标记从运行时中删除
                await ctx.database.remove('w-repeat-record', resumed.id)
                rec.deleted = true

                // 用恢复的复读替换当前复读
                isNewRec = false
                runtime.currentRec = omit(resumed, [ 'id' ])
            }
        })

        // 如果发生复读，将当前用户加入运行时的发送者列表中
        if (isRepeating) currentRec.senders.push(uid)
        // 当前复读的复读条数，大于 1 则为完整复读
        const repeatCount = currentRec?.senders?.length ?? 0

        // 如果发生了复读
        if (isRepeating) {
            // 如果是完整复读，则更新用户复读数据
            if (repeatCount > 1) await Promise.all([
                // 更新当前用户的复读数据
                ctx.database.upsert('w-repeat-user', row => [{
                    uid,
                    repeatCount: $inc(row.repeatTime),
                    repeatTime: currentRec.senders.slice(0, -1).includes(uid)
                        ? undefined
                        : $inc(row.repeatTime)
                }]),
                // 更新复读发起者的复读数据
                ctx.database.upsert('w-repeat-user', row => [{
                    uid: currentRec.senders[0],
                    beRepeatedCount: $inc(row.beRepeatedCount),
                    beRepeatedTime: currentRec.senders.length === 2
                        ? $inc(row.beRepeatedTime)
                        : undefined
                }])
            ])
        }
        else {
            // 处理未激活的复读
            await procRecs('queuedRecs', async rec => {
                // 用新激活的复读替换当前复读
                isNewRec = false
                currentRec = runtime.currentRec = rec
            })

            // 如果当前复读是完整复读（即发送人数大于 1），则被打断
            if (repeatCount > 1) {
                // 记录打断者和复读结束时间
                currentRec.interrupter = uid
                currentRec.endTime = Date.now()

                const [ old ] = await Promise.all([
                    // 将运行时作为新复读记录写入复读记录表
                    ctx.database.create('w-repeat-record', omit(currentRec, [ 'unrelatedCount' ])),
                    // 更新打断者复读用户数据
                    ctx.database.upsert('w-repeat-user', row => [{
                        uid,
                        interruptTime: $inc(row.interruptTime)
                    }]),
                    // 识别图片中文字
                    (config.doProcessImage && config.doOcr && tesseractWorker) ? updateImageText(currentRec) : undefined
                ])

                // 如果允许挂起，挂起被打断的复读
                if (config.maxUnrelatedCount && old.senders.length > 1) {
                    runtime.suspendedRecs.unshift({
                        ...old,
                        unrelatedCount: 1,
                        suspendTime: Date.now(),
                        resumeTime: undefined
                    })
                }
            }

            // 如果需要新建当前复读
            if (isNewRec) {
                // 新建当前复读，包含内容、图片、开始时间、第一个发送者的信息
                runtime.currentRec = currentRec = {
                    ...createCurrentRec(gid),
                    content,
                    images,
                    startTime: session.timestamp,
                    senders: [ uid ]
                }
                // 滚动复读队列
                runtime.queuedRecs.unshift(currentRec)
            }
        }

        // 机器人复读
        if (currentRec.senders.length === config.repeatCount) return unescapeMessage(thisMessage)

        // 传向下一个中间件
        return next()
    }, true)

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
            const isFiltered = ([ 'filter', 'starter', 'repeater', 'interrupter' ] satisfies (keyof typeof options)[])
                .some(name => name in options)

            // TODO: wait for row destruction
            const reserveProjection = getReserveProjection<RepeatRecord>([
                'id', 'gid', 'content', 'senders', 'startTime', 'endTime', 'interrupter', 'images'
            ])

            let recs = await ctx.database
                .select('w-repeat-record')
                .where(row => $.query(row, {
                    gid: isGlobal ? {} : gid,
                    ...parseDuration(duration)
                }, $.and(
                    options.filter
                        ? $.regex(row.content, options.filter)
                        : true,
                    options.starter
                        ? $.eq($.get(row.senders, 0), options.starter)
                        : true,
                    options.repeater
                        ? $.in(options.repeater, row.senders)
                        : true,
                    options.interrupter
                        ? $.eq(row.interrupter, options.interrupter)
                        : true
                )))
                .project({
                    ...reserveProjection,
                    count: row => $.length(row.senders)
                })
                .project({
                    ...reserveProjection,
                    count: row => row.count,
                    tps: row => $.mul($.div(row.count, $.sub(row.endTime, row.startTime)), 1000)
                })
                .orderBy(sortMethod, sortDirection)
                .execute()

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
                    const extra = sortMethod === 'tps' ? `, ${rec.tps.toFixed(2)}/s` : ''
                    return `${i + 1}. [${content}${times}${extra}] # ${rec.id}`
                })
                .join('\n')

            return (options.list
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
            const recs = await ctx.database
                .select('w-repeat-record')
                .where({
                    gid,
                    ...parseDuration(options.duration)
                })
                .execute()

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

            const starters = Object.entries(starterDict)
                .map(([ uid, { name, count } ]) => ({ uid, name, count }))
            const starterNum = starters.length
            const maxRepeatedCount = Math.max(...starters.map(({ count }) => count))
            const nodes = starters
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
                    symbolSize: count / maxRepeatedCount * 150,
                    category: String(i)
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
                        .from({ length: starterNum })
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
            const recs = await ctx.database
                .select('w-repeat-record')
                .where({ gid })
                .project([ 'startTime' ])
                .execute()

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

    ctx.command('repeat.record <id:posint>', '查看某次复读详情')
        .alias('repeat.r')
        .option('all-senders', '-a 显示所有参与者')
        .option('suspension', '-s 显示挂起详情')
        .option('delete', '-d 删除此复读详情', { authority: 4 })
        .option('ocr', '-o 识别图片中文字')
        .action(async ({ session, options }, id) => {
            const [ rec ] = await ctx.database.get('w-repeat-record', { id })
            if (! rec) return `未找到复读 #${id}。`

            const guildId = rec.gid.split(':')[1]
            const memberDict = await getMemberDict(session, guildId)
            const guild = await session.bot.getGuild(guildId)

            let content: string

            if (options.delete) {
                await ctx.database.remove('w-repeat-record', { id })
                content = '[已删除]'
            }
            else {
                content = unescapeMessage(rec)
                if (options.ocr) {
                    if (tesseractWorker) await updateImageText(rec)
                    else return 'tesseract 服务未加载，无法识别图片中文字'
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
                内容：${options.delete ? '[已删除]' : content}${rec.images.length || options.ocr
                    ? `\n图片识别结果：${options.ocr ? '[新识别]' : ''}\n${rec.images
                        .map(({ text }, i) => `${i + 1}. ${text.trim() || '[未识别到文字]'}`)
                        .join('\n')
                    }`
                    : ''
                }
            `
        })

    ctx.command('repeat.debug', '复读调试', { hidden: true })

    ctx.command('repeat.debug.eval <code:text>', '在本插件作用域中运行 JavaScript', { authority: 4 })
        .action(async (_, code) => {
            try {
                return format(await eval(code))
            }
            catch (error) {
                return format(error)
            }
        })

    ctx.command('repeat.debug.runtime', '获取当前复读运行时', { authority: 2 })
        .action(({ session }) => '当前运行时：\n' + h.escape(format(runtimes[session.gid])))

    ctx.command('repeat.debug.runtime.clear', '清除复读运行时', { authority: 2 })
        .option('all', '-a 清除所有')
        .action(async ({ session: { gid }, options: { all } }) => {
            const gids = all ? Object.keys(runtimes) : [gid]
            gids.forEach(gid => delete runtimes[gid])

            return `清除了 ${gids.length} 个运行时`
        })

    ctx.command('repeat.debug.runtime.list', '获取复读运行时列表', { authority: 2 })
        .action(() => '运行时列表：' + Object.keys(runtimes).join(', '))

    ctx.command('repeat.admin', '复读管理', { hidden: true })
        .alias('repeat.a')

    ctx.command('repeat.admin.conf', '查看复读配置')
        .option('switch', '-S 开关复读写入', { authority: 4 })
        .action(({ options }) => {
            if (options.switch) {
                config.doWrite = ! config.doWrite
                ctx.scope.update(config)
            }
            return dedent`
                复读配置
                ====================
                复读写入：${config.doWrite}
                处理图片：${config.doProcessImage}
                自动识别图片：${config.doOcr}
                复读内容黑名单：${config.repeatBlacklist.map(re => `/${re}/`).join(', ')}
            `
        })

    ctx.command('repeat.admin.regen-user-table', '重建复读用户表', { authority: 4 })
        .action(async ({ session }) => {
            await session.send('正在根据复读记录重建用户数据表……')
            await ctx.database.remove('w-repeat-user', {})
            const recs = await ctx.database.get('w-repeat-record', {})
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

                getUser(rec.interrupter).interruptTime ++
            })
            const writeResult = await ctx.database.upsert('w-repeat-user', () => Object.values(users))
            return `已重建 ${writeResult.inserted} 条用户数据`
        })

    ctx.command('repeat.admin.recognize-all-images', '识别所有表中图片', { authority: 4 })
        .action(({ session }) => profile(async () => {
            if (! ctx.tesseract) return 'tesseract 服务未加载，无法识别图片中文字'
            await session.send('开始查询数据库……')
            const recs = await ctx.database.get('w-repeat-record', row => $.gt($.length(row.images), 0))
            const imageCount = recs.reduce((count, rec) => count + rec.images.filter(x => x !== null).length, 0)
            await session.send(`正在识别 ${recs.length} 条复读记录中的 ${imageCount} 张图片……`)
            await Promise.all(recs.map(rec => updateImageText(rec)))
        }))

    ctx.command('repeat.admin.migrate-guild <from:channel> <to:channel>', '迁移群复读记录', { authority: 4 })
        .action(async (_, from, to) => {
            const res = await ctx.database.set('w-repeat-record', { gid: from }, { gid: to })
            return `成功从 ${from} 迁移了 ${res.modified} 条复读记录到 ${to}。`
        })

    // 回收副作用
    ctx.on('dispose', () => {
        // 终止 tesseract Worker
        tesseractWorker?.terminate()
    })
}
