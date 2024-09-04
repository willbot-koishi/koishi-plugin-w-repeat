import { $, h, Argv, Context, Direction, Field, Row, Session, z } from 'koishi'
import { type GuildMember } from '@satorijs/protocol'

import {} from '@koishijs/plugin-help'
import { type Reactive } from 'koishi-plugin-w-reactive'
import {} from 'koishi-plugin-w-option-conflict'
import {} from 'koishi-plugin-w-echarts'

import dedent from 'dedent'
import dayjs from 'dayjs'
import format from 'pretty-format'
import { join } from 'path'

export const name = 'w-repeat'

export const inject = {
    required: [ 'database', 'reactive' ],
    optional: [ 'echarts' ]
}

export interface Config {
    repeatTime?: number
    displayLength?: number
    displayPageSize?: number
    repeatBlacklist?: string[]
    doProcessImage?: boolean
    doWrite?: boolean
}

export const Config: z<Config> = z.object({
    repeatTime: z.number().min(0).default(0).description('机器人复读需要的次数，0 为不复读'),
    displayLength: z.number().min(0).default(25).description('复读消息最大长度，超过则显示为省略号'),
    displayPageSize: z.number().min(5).default(10).description('复读消息分页大小，至少为 5'),
    repeatBlacklist: z.array(z.string()).description('复读内容黑名单'),
    doProcessImage: z.boolean().default(false).description('是否处理图片（会使用较多数据库空间）'),
    doWrite: z.boolean().default(true).description('是否向数据库写入复读数据'),
})

declare module 'koishi' {
    interface Tables {
        'w-repeat-record': RepeatRecord         // 复读记录表
        'w-repeat-runtime': RepeatRuntime       // 复读运行时（正在进行的复读）表
        'w-repeat-user': RepeatUser             // 复读用户表
    }
}

export interface RepeatRecord {
    id: number
    gid: string
    content?: string
    senders?: string[]
    startTime?: number
    endTime?: number
    interrupter?: string
    images?: string[]
}

export interface RepeatRuntime extends RepeatRecord {}

export interface RepeatUser {
    uid: string
    repeatTime: number
    repeatCount: number
    beRepeatedTime: number
    beRepeatedCount: number
    interruptTime: number
}

export function apply(ctx: Context, config: Config) {
    // 扩展数据库模型

    const repeatFields = {
        id: 'unsigned',
        gid: 'string',
        content: 'string',
        senders: 'array',
        startTime: 'unsigned',
        endTime: 'unsigned',
        interrupter: 'string',
        images: 'array'
    } as const satisfies Field.Extension<RepeatRecord>
    ctx.model.extend('w-repeat-record', { ...repeatFields }, { autoInc: true })
    ctx.model.extend('w-repeat-runtime', { ...repeatFields }, { autoInc: true })

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

    // 工具函数

    const ellipsis = (text: string, maxLength: number) => text.length < maxLength
        ? text
        : text.slice(0, maxLength - 3) + '...'

    const maybeArray = <T>(x: T | T[]): T[] => Array.isArray(x) ? x : [ x ]

    const countBy = <T extends {}, K extends keyof any>(xs: T[], key: (x: T) => K | K[]) =>
        xs.reduce<Record<keyof any, number>>((dict, x) => {
            maybeArray(key(x)).forEach(k => {
                dict[k] ??= 0
                dict[k] ++
            })
            return dict
        }, {})

    const countAndSortBy = <T extends {}, K extends keyof any>(xs: T[], key: (x: T) => K | K[]) =>
        Object.entries(countBy(xs, key)).sort(([, count1 ], [, count2]) => count2 - count1)

    const getMemberDict = async (session: Session, guildId: string) => {
        const { data: memberList } = await session.bot.getGuildMemberList(guildId)
        return memberList.reduce<Record<string, GuildMember>>((dict, member) => {
            dict[session.platform + ':' + member.user.id] = member
            return dict
        }, {})
    }

    const getMemberName = (member: GuildMember) =>
        member.nick || member.name || member.user.name || member.user.id
    
    const requireList = (): Argv.OptionConfig => ({
        conflictsWith: { option: 'list', value: false }
    })

    const getReserveProjection = <S>(fields: Record<keyof S, any>): {
        [K in keyof S]: (row: Row<S>) => Row<S>[K]
    } => Object.fromEntries(Object
        .keys(fields)
        .map(name => [ name, (row: Row<S>) => row[name] ])
    ) as any

    const omit = <T, K extends keyof T>(x: T, keys: K[]): Omit<T, K> => {
        const res: T = { ...x }  
        keys.forEach(k => delete res[k])
        return res as Omit<T, K>
    }

    const streamToBuffer = async (stream: ReadableStream<Uint8Array>): Promise<Buffer> => {
        const buffers: Uint8Array[] = []
        for await (const data of stream) buffers.push(data)
        return Buffer.concat(buffers)
    } 

    const $inc = (expr: $.Expr) => $.add(expr, 1)

    const isSameImages = (images1: string[], images2: string[]): boolean => {
        return images1.length === images2.length
            && images1.every((b1, i) => {
                const b2 = images2[i]
                return b1 === b2
            })
    }

    // 复读中间件

    const runtimes: Record<string, Reactive<RepeatRuntime>> = {}
    ctx.middleware(async (session, next) => {
        // 配置为不写入则跳过
        if (! config.doWrite) return next()

        // 只处理群内消息
        const { content: originalContent, gid, uid } = session
        if (! session.guildId) return next()

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
        const images = await Promise.all(imageSrcs.map(async src => {
            try {
                const res = await fetch(src)
                const buffer = await streamToBuffer(res.body)
                return buffer.toString('base64')
            }
            catch (err) {
                ctx.logger.error('Failed download image <%s>: %s', src, err)
                return null
            }
        }))

        // 获取本群复读运行时，若无则创建
        const { reactive: runtime, patch } = runtimes[gid]
            ??= await ctx.reactive.create('w-repeat-runtime', { gid }, { gid })

        // 一次性写入运行时变化
        let repeatCount = 0
        await patch(async (raw) => {
            // 如果运行时中还不存在消息，或者记录的消息与当前消息不同
            if (! raw.content || raw.content !== content || ! isSameImages(images, raw.images)) {
                // 如果运行时中的消息有不止一位发送者，即已经构成复读
                if (raw.content && raw.senders.length > 1) {
                    // 记录打断者和复读结束时间，并将运行时作为新复读记录写入复读记录表
                    raw.interrupter = uid
                    raw.endTime = Date.now()
                    ctx.database.create('w-repeat-record', omit(raw, [ 'id' ]))
                    // 更新打断者复读用户数据
                    await ctx.database.upsert('w-repeat-user', row => [ {
                        uid,
                        interruptTime: $inc(row.interruptTime)
                    } ])
                }

                // 更新运行时的消息、附带图片、开始时间和发送者
                raw.content = content
                raw.images = images
                raw.startTime = Date.now()
                raw.senders = []
            }

            // 将当前用户加入运行时的发送者列表中
            raw.senders.push(uid)

            // 保存当前状态复读信息，防止消息间隔过短
            repeatCount = raw.senders.length
        })

        // 如果发生了复读
        if (repeatCount > 1) await Promise.all([
            // 更新当前用户的复读数据
            ctx.database.upsert('w-repeat-user', row => [ {
                uid,
                repeatCount: $inc(row.repeatTime),
                repeatTime: runtime.senders.slice(0, -1).includes(uid)
                    ? undefined
                    : $inc(row.repeatTime)
            } ]),
            // 更新复读发起者的复读数据
            ctx.database.upsert('w-repeat-user', row => [ {
                uid: runtime.senders[0],
                beRepeatedCount: $inc(row.beRepeatedCount),
                beRepeatedTime: runtime.senders.length === 2
                    ? $inc(row.beRepeatedTime)
                    : undefined
            } ])
        ])

        // 如果复读次数达到配置，机器人参与复读
        if (repeatCount === config.repeatTime) {
            return session.content
        }

        return next()
    }, true)

    // 复读指令

    ctx.command('repeat.me', '查看我的复读统计')
        .action(async ({ session: { uid } }) => {
            const [ user ] = await ctx.database.get('w-repeat-user', { uid })
            if (! user) return `还没有复读统计`
            return dedent`
                复读条数: ${user.repeatCount}
                复读次数: ${user.repeatTime}
                被复读条数：${user.beRepeatedCount}
                被复读次数：${user.beRepeatedTime}
                打断复读次数: ${user.interruptTime}
            `
        })

    ctx.command('repeat.stat', '查看群复读统计')
        .alias('repeat.guild')
        .option('guild', '-g <guild:channel> 指定群（默认为本群）')
        .option('duration', '-d <duration> 指定时间范围，可以为 day / week / month / all', {
            type: /^(day|week|month|all)$/, fallback: 'day'
        })
        .option('page', '-p <page:posint> 查看分页', { fallback: 1 })
        .option('list', '-l 显示复读记录列表', { fallback: true })
        .option('list', '-L 不显示复读记录列表', { value: false })
        .option('top', '-t <top:natural> 排行榜人数', { fallback: 1 })
        .option('filter', '-f <content:string> 根据查找复读记录', requireList())
        // Todo: wait for minato array[index] query
        // .option('starter', '-s [user:user] 根据发起者查找（默认为自己）', requireList())
        // .option('repeater', '-r [user:user] 根据参与者查找（默认为自己）', requireList())
        // .option('interrupter', '-i [user:user] 根据打断者查找（默认为自己）', requireList())
        .option('sort', '-s <sortby> 指定排序方式', { type: /^(count|tps|startTime)?(:(desc|asc))?$/, fallback: 'count' })
        .action(async ({ session, options }) => {
            if (! session.guildId && ! options.guild) return '请在群内调用'
            const gid = options.guild ?? session.gid
            const [, guildId ] = gid.split(':')

            const { filter, top: topNum } = options 
            const duration = options.duration as 'day' | 'week' | 'month' | 'all'
            const [ sortMethod = 'count', sortDirection = 'desc' ] = options.sort.split(':') as [
                'count' | 'tps' | 'startTime', Direction
            ]

            const reserveProjection = getReserveProjection<RepeatRecord>(repeatFields)

            const recs = await ctx.database
                .select('w-repeat-record')
                .where({
                    gid,
                    startTime: duration === 'all'
                        ? {}
                        : { $gte: + dayjs().startOf(duration) },
                    content: filter
                        ? { $regex: new RegExp(filter) }
                        : {}
                })
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

            const topInterrupters = countAndSortBy(recs, rec => rec.interrupter)
            const topStarters = countAndSortBy(recs, rec => rec.senders[0])
            const topRepeaters = countAndSortBy(recs, rec => rec.senders)

            const memberDict = await getMemberDict(session, guildId)

            const topText = (action: string, tops: [ string, number ][]) =>  dedent`
                ${action}最多的${ topNum > 1 ? ` ${topNum} 名群友` : '' }是：${ tops
                    .slice(0, topNum)
                    .map(([ uid, count ]) => `[${ getMemberName(memberDict[uid]) } * ${count}]`)
                    .join(', ')
                }
            `
            const { [duration]: durationText } = {
                'all': '',
                'day': '今日',
                'week': '这周',
                'month': '当月'
            } satisfies Record<typeof duration, string>
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
            const groupText = options.guild
                ? (await session.bot.getGuild(gid.split(':')[1])).name
                : '本群'
            if (! total) return `${groupText}${durationText}还没有复读。在？为什么不复读？`

            const { displayPageSize: pageSize, displayLength } = config
            const pageNum = Math.ceil(total / pageSize)
            const pageId = options.page
            if (pageId < 1 || pageId > pageNum) return `页数必须为 1 到 ${pageNum} 间的整数。`

            return (options.list
                ? dedent`
                    ${groupText}${durationText}共有 ${ recs.length } 次${ options.filter ? `符合 /${options.filter}/ 的` : '' }复读
                    按${sortMethodText}${sortDirectionText}排序依次为：（第 ${pageId} / ${pageNum} 页）
                    ${ recs
                        .slice((pageId - 1) * pageSize, pageId * pageSize)
                        .map((rec, i) => {
                            const content = ellipsis(rec.content.replace(/@__KOISHI_IMG__@/g, '[图片]'), displayLength)
                            const times = ` * ${rec.senders.length}`
                            const extra = sortMethod === 'tps' ? `, ${rec.tps.toFixed(2)}/s` : ''
                            return `${i + 1}. [${content}${times}${extra}] # ${rec.id}`
                        })
                        .join('\n')
                    }
                ` + '\n\n'
                : ''
            ) + (filter || topNum === 0
                ? ''
                : dedent`   
                    ${ topText('参与复读', topRepeaters) }
                    ${ topText('发起复读', topStarters) }
                    ${ topText('打断复读', topInterrupters) }
                `
            )
        })

    ctx.command('repeat.graph', '查看复读各类图表')

    ctx.command('repeat.graph.flow', '查看群复读流向图')
        .option('guild', '-g <guild:channel> 指定群（默认为本群）')
        .option('duration', '-d <duration> 指定时间范围，可以为 day / week / month / all', {
            type: /^(day|week|month|all)$/, fallback: 'day'
        })
        .action(async ({ session, options }) => {
            if (! ctx.echarts) return '此指令需要 echarts 服务'

            if (! session.guildId && ! options.guild) return '请在群内调用'
            const gid = options.guild ?? session.gid
            const [, guildId ] = gid.split(':')

            const memberDict = await getMemberDict(session, guildId)

            const duration = options.duration as 'day' | 'week' | 'month' | 'all'

            const starters: Record<string, { name: string, count: number }> = {}
            const sendMat: Record<string, Record<string, { count: number }>> = {}
            const recs = await ctx.database
                .select('w-repeat-record')
                .where({
                    gid,
                    startTime: duration === 'all'
                        ? {}
                        : { $gte: + dayjs().startOf(duration) }
                })
                .execute()

            recs.forEach(rec => {
                const starter = rec.senders[0]
                ; (starters[starter] ??= {
                    name: getMemberName(memberDict[starter]),
                    count: 0
                }).count ++
                rec.senders.slice(1).forEach(sender => {
                    ((sendMat[sender] ??= {})[starter] ??= { count: 0 }).count ++
                })
            })
            const starterNum = Object.keys(starters).length

            type GraphSeriesOption = echarts.RegisteredSeriesOption['graph']

            // Todo: normalization

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
                    categories: Array.from({ length: starterNum }).map((_, i) => ({ name: String(i) })),
                    data: Object
                        .entries(starters)
                        .map<GraphSeriesOption['data'][number]>(([ uid, { name, count } ], i) => ({
                            name: uid,
                            label: {
                                show: true,
                                formatter: name,
                                color: '#000',
                                borderColor: 'transparent',
                                shadowColor: 'transparent',
                                fontSize: 22
                            },
                            symbolSize: count * 4,
                            category: String(i)
                        })),
                    links: Object
                        .entries(sendMat)
                        .flatMap(([ source, targetRow ]) => (
                            Object.entries(targetRow).map<GraphSeriesOption['links'][number]>(([ target, { count } ]) => ({
                                source,
                                target,
                                lineStyle: {
                                    width: count * 2,
                                    curveness: 0.2,
                                    type: 'solid',
                                    color: 'source'
                                }
                            }))
                        ))
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

            // Todo: optimize
            const recs = await ctx.database
                .select('w-repeat-record')
                .where({ gid })
                .project([ 'startTime' ])
                .execute()

            const timeMat: Record<string, Record<string, number>> = Object.fromEntries(
                Array.from({ length: 24 }).map((_, i) => [
                    i,
                    Object.fromEntries(Array.from({ length: 7 }).map((_, j) => [ j, 0 ]))
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
                    orient: 'horizontal',
                    left: 'center',
                    bottom: '10'
                },
                series: {
                    type: 'heatmap',
                    silent: true,
                    label: { show: true },
                    data
                }
            })

            return eh.export()
        })

    ctx.command('repeat.record <id:posint>', '查看某次复读详情')
        .option('all-senders', '-a 显示所有参与者')
        .option('delete', '-d 删除此复读详情', { authority: 4 })
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
                let i = 0
                content = rec.content.replace(/@__KOISHI_IMG__@/g, () => {
                    return h.img('data:image/png;base64,' + rec.images[i ++]).toString()
                })
            }
            
            return dedent`
                复读 #${id} 详情
                群：${guild.name}${ guildId === session.guildId ? '（本群）' : '' }
                发起者：${ getMemberName(memberDict[rec.senders[0]]) }
                发起时间：${ dayjs(rec.startTime).format('YYYY/MM/DD HH:mm:ss') }
                打断者：${ getMemberName(memberDict[rec.interrupter]) }
                打断时间：${ dayjs(rec.endTime).format('YYYY/MM/DD HH:mm:ss') }${
                    options['all-senders']
                    ? `\n参与者：${ rec.senders.map(uid => getMemberName(memberDict[uid])).join('，') }`
                    : ''
                }
                内容：${ options.delete ? '[已删除]' : content }
            `
        })

    ctx.command('repeat.conf', '查看复读配置')
        .option('switch', '-S 开关复读写入', { authority: 4 })
        .action(({ options }) => {
            if (options.switch) {
                config.doWrite = ! config.doWrite
                ctx.scope.update(config)
            }
            return dedent`
                复读配置
                ==========
                已开启复读写入：${config.doWrite}
                复读内容黑名单：${config.repeatBlacklist.map(re => `/${re}/`).join(', ')}
            `
        })

    ctx.command('repeat.debug', '复读调试', { hidden: true })

    ctx.command('repeat.debug.runtime', '获取当前复读运行时', { authority: 2 })
        .action(({ session }) => 'Current runtime: '
            + h.escape(format(runtimes[session.gid]?.reactive))
        )

    ctx.command('repeat.debug.runtime.list', '获取复读运行时列表', { authority: 2 })
        .action(() => 'Runtime list: ' + Object.keys(runtimes).join(', '))

    ctx.command('repeat.debug.regen-user-table', '重建复读用户表', { authority: 4 })
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

    ctx.on('dispose', () => {
        Object.values(runtimes).forEach(reactive => reactive.dispose())
    })
}
