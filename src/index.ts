import { $, h, Argv, Context, Direction, Field, Row, Session, z } from 'koishi'
import { type GuildMember } from '@satorijs/protocol'

import { type Reactive } from 'koishi-plugin-w-reactive'
import {} from 'koishi-plugin-w-option-conflict'
import {} from 'koishi-plugin-w-echarts'

import dedent from 'dedent'
import dayjs from 'dayjs'

export const name = 'w-repeat'

export const inject = {
    required: [ 'database', 'reactive' ],
    optional: [ 'echarts' ]
}

export interface Config {
    repeatTime?: number
    displayLength?: number
    displayPageSize?: number
}

export const Config: z<Config> = z.object({
    repeatTime: z.number().min(0).default(0).description('机器人复读需要的次数，0 为不复读'),
    displayLength: z.number().min(0).default(25).description('复读消息最大长度，超过则显示为省略号'),
    displayPageSize: z.number().min(5).default(10).description('复读消息分页大小，至少为 5')
})

declare module 'koishi' {
    interface Tables {
        'w-repeat-record': RepeatRecord
        'w-repeat-runtime': RepeatRuntime
        'w-repeat-user': RepeatUser
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
    const repeatFields = {
        id: 'unsigned',
        gid: 'string',
        content: 'string',
        senders: 'array',
        startTime: 'unsigned',
        endTime: 'unsigned',
        interrupter: 'string'
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

    const reserveProjection = <S>(fields: Record<keyof S, any>): {
        [K in keyof S]: (row: Row<S>) => Row<S>[K]
    } => Object.fromEntries(Object
        .keys(fields)
        .map(name => [ name, (row: Row<S>) => row[name] ])
    ) as any

    const runtimes: Record<string, Reactive<RepeatRuntime>> = {}
    ctx.middleware(async (session, next) => {
        const { content, gid, uid } = session
        if (! session.guildId) return next()

        if ((ctx.root.config.prefix as string[]).some(pre => content.startsWith(pre))) return next()

        const { reactive: runtime, patch } = runtimes[gid]
            ??= await ctx.reactive.create('w-repeat-runtime', { gid }, { gid })

        const $inc = (expr: $.Expr) => $.add(expr, 1)

        await patch(async (raw) => {
            if (! raw.content || raw.content !== content) {
                if (raw.content && raw.senders.length > 1) {
                    raw.interrupter = uid
                    raw.endTime = Date.now()
                    ctx.database.create('w-repeat-record', raw)
                    await ctx.database.upsert('w-repeat-user', row => [ {
                        uid,
                        interruptTime: $inc(row.interruptTime)
                    } ])
                }

                raw.content = content
                raw.startTime = Date.now()
                raw.senders = []
            }

            raw.senders.push(uid)
        })

        if (runtime.senders.length > 1) await Promise.all([
            ctx.database.upsert('w-repeat-user', row => [ {
                uid,
                repeatCount: $inc(row.repeatTime),
                repeatTime: runtime.senders.slice(0, -1).includes(uid)
                    ? undefined
                    : $inc(row.repeatTime)
            } ]),
            ctx.database.upsert('w-repeat-user', row => [ {
                uid: runtime.senders[0],
                beRepeatedCount: $inc(row.beRepeatedCount),
                beRepeatedTime: runtime.senders.length === 2
                    ? $inc(row.beRepeatedTime)
                    : undefined
            } ])
        ])

        if (runtime.senders.length === config.repeatTime) {
            return session.content
        }

        return next()
    }, true)

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
        // Todo: wait for array[index] query
        // .option('starter', '-s [user:user] 根据发起者查找（默认为自己）', requireList())
        // .option('repeater', '-r [user:user] 根据参与者查找（默认为自己）', requireList())
        // .option('interrupter', '-i [user:user] 根据打断者查找（默认为自己）', requireList())
        .option('sort', '-s <sortby> 指定排序方式', { type: /^(times|tps)?(:(desc|asc))?$/, fallback: 'times' })
        .action(async ({ session, options }) => {
            if (! session.guildId && ! options.guild) return '请在群内调用'
            const gid = options.guild ?? session.gid
            const [, guildId ] = gid.split(':')

            const { filter, top: topNum } = options 
            const duration = options.duration as 'day' | 'week' | 'month' | 'all'
            const [ sortMethod = 'times', sortDirection = 'desc' ] = options.sort.split(':') as [ 'times' | 'tps', Direction ]

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
                    ...reserveProjection<RepeatRecord>(repeatFields),
                    times: row => $.length(row.senders)
                })
                .project({
                    ...reserveProjection<RepeatRecord>(repeatFields),
                    times: row => row.times,
                    tps: row => $.mul($.div(row.times, $.sub(row.endTime, row.startTime)), 1000)
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
                'times': '复读次数',
                'tps': '每秒复读次数'
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
                            const content = ellipsis(rec.content, displayLength)
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

            type GraphSeriesOption = echarts.RegisteredSeriesOption['graph']

            // Todo: normalization

            const CATEGORY_NUM = 6
            const eh = ctx.echarts.createChart(800, 800, {
                series: {
                    type: 'graph',
                    layout: 'circular',
                    circular: {
                        rotateLabel: true
                    },
                    emphasis: {
                        focus: 'adjacency'
                    },
                    categories: Array.from({ length: CATEGORY_NUM }).map((_, i) => ({ name: String(i) })),
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
                            category: String(i % CATEGORY_NUM)
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
            const [, guildId ] = gid.split(':')

            // Todo: optimize
            const recs = await ctx.database
                .select('w-repeat-record')
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

    ctx.command('repeat.show <id:posint>', '查看某次复读详情')
        .option('guild', '-g <guild:channel> 指定群（默认为本群）')
        .action(async ({ session }, id) => {
            const [ rec ] = await ctx.database.get('w-repeat-record', { id })
            if (! rec) return `未找到复读 #${id}。`

            const guildId = rec.gid.split(':')[1]
            const memberDict = await getMemberDict(session, guildId)
            const guild = await session.bot.getGuild(guildId)

            return dedent`
                复读 #${id} 详情
                群：${guild.name}${ guildId === session.guildId ? '（本群）' : '' }
                发起者：${getMemberName(memberDict[rec.senders[0]])}
                发起时间：${dayjs(rec.startTime).format('YYYY/MM/DD HH:mm:ss')}
                打断者：${getMemberName(memberDict[rec.interrupter])}
                打断时间：${dayjs(rec.endTime).format('YYYY/MM/DD HH:mm:ss')}
                内容：${rec.content}
            `
        })

    ctx.command('repeat.debug.runtimes', { authority: 2 })
        .action(() => Object.keys(runtimes).join(', '))

    ctx.command('repeat.debug.regenUserTable', { authority: 4 })
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
