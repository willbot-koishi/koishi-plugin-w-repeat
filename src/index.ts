import { $, Context, z } from 'koishi'
import { Reactive } from 'koishi-plugin-w-reactive'
import dedent from 'dedent'
import dayjs from 'dayjs'
import { GuildMember } from '@satorijs/protocol'

export const name = 'w-repeat'

export const inject = [ 'database', 'reactive' ]

export interface Config {
    repeatTime?: number
}

export const Config: z<Config> = z.object({
    repeatTime: z.number().min(0).default(0).description('机器人复读需要的次数，0 为不复读')
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
    id: number
    uid: string
    repeatTime: number
    repeatCount: number
    beRepeatedTime: number
    beRepeatedCount: number
    interruptTime: number
}

export function apply(ctx: Context, config: Config) {
    const repeatFields = () => ({
        id: 'unsigned',
        gid: 'string',
        content: 'string',
        senders: 'array',
        startTime: 'unsigned',
        endTime: 'unsigned',
        interrupter: 'string'
    }) as const
    ctx.model.extend('w-repeat-record', repeatFields(), { autoInc: true })
    ctx.model.extend('w-repeat-runtime', repeatFields(), { autoInc: true })

    const counterField = () => ({
        type: 'unsigned',
        initial: 0
    } as const)
    ctx.model.extend('w-repeat-user', {
        id: 'unsigned',
        uid: 'string',
        repeatTime: counterField(),
        repeatCount: counterField(),
        beRepeatedTime: counterField(),
        beRepeatedCount: counterField(),
        interruptTime: counterField()
    }, { autoInc: true })

    const runtimes: Record<string, Reactive<RepeatRuntime>> = {}

    ctx.middleware(async (session, next) => {
        const { content, gid, uid } = session
        if (! session.guildId) return next()

        const { reactive: runtime, patch } = runtimes[gid]
            ??= await ctx.reactive.create('w-repeat-runtime', { gid }, { gid })

        const $inc = (expr: $.Expr) => $.add(expr, 1)

        await patch((raw) => {
            if (! raw.content || raw.content !== content) {
                if (raw.content && raw.senders.length > 1) {
                    raw.interrupter = uid
                    raw.endTime = Date.now()
                    ctx.database.create('w-repeat-record', raw)
                    ctx.database.set('w-repeat-user', { uid }, row => ({
                        interruptTime: $inc(row.interruptTime)
                    }))
                }

                raw.content = content
                raw.startTime = Date.now()
                raw.senders = []
            }

            raw.senders.push(uid)
        })

        if (runtime.senders.length > 1) {
            await ctx.database.upsert('w-repeat-user', row => [ {
                uid,
                repeatCount: $inc(row.repeatTime),
                repeatTime: runtime.senders.slice(0, -1).includes(uid)
                    ? undefined
                    : $inc(row.repeatTime)
            } ])
            await ctx.database.upsert('w-repeat-user', row => [ {
                uid: runtime.senders[0],
                beRepeatedTime: runtime.senders.length === 2
                    ? $inc(row.beRepeatedTime)
                    : undefined,
                beRepeatedCount: $inc(row.beRepeatedCount)
            } ])
        }

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

    ctx.command('repeat.group', '查看群复读统计')
        .option('duration', '-d <duration>', { type: /^day|week|month|all$/, fallback: 'day' })
        .action(async ({ session, options }) => {
            const { gid, guildId } = session
            if (! guildId) return '请在群内调用'

            const duration = options.duration as 'day' | 'week' | 'month' | 'all'
            const recs = await ctx.database
                .select('w-repeat-record')
                .where({
                    gid,
                    startTime: duration === 'all'
                        ? undefined
                        : { $gte: + dayjs().startOf(duration) }
                })
                .orderBy(row => $.length(row.senders), 'desc')
                .execute()

            const [ topInterrupter ] = countAndSortBy(recs, rec => rec.interrupter)
            const [ topStarter ] = countAndSortBy(recs, rec => rec.senders[0])
            const [ topRepeater ] = countAndSortBy(recs, rec => rec.senders)

            const { [duration]: durationText } = {
                'all': '',
                'day': '今日',
                'week': '这周',
                'month': '当月'
            } satisfies Record<typeof duration, string>

            const { data: memberList } = await session.bot.getGuildMemberList(guildId)
            const memberDict = memberList.reduce<Record<string, GuildMember>>((dict, member) => {
                dict[member.user.id] = member
                return dict
            }, {})

            const topText = (action: string, [ uid, count ]: [ string, number ]) => {
                const userId = uid.split(':')[1]
                const member = memberDict[userId]
                const name = member?.nick || member?.name || userId
                return `${action}最多的人是 ${name}（${count} 次）`
            }

            return recs.length
                ? dedent`
                    本群${durationText}共有 ${ recs.length } 次复读，按复读次数排序依次为（最多显示 10 条）：
                    ${ recs
                        .slice(0, 10)
                        .map((rec, i) => `${i + 1}. ${ ellipsis(rec.content, 20) } * ${rec.senders.length}`)
                        .join('\n')
                    }
                    
                    ${ topText('参与复读', topRepeater) }
                    ${ topText('发起复读', topStarter) }
                    ${ topText('打断复读', topInterrupter) }
                `
                : `本群${durationText}还没有复读。在？为什么不复读？`
        })

    ctx.command('repeat.debug.runtimes')
        .action(() => Object.keys(runtimes).join(', '))

    ctx.on('dispose', () => {
        Object.values(runtimes).forEach(reactive => reactive.dispose())
    })
}

