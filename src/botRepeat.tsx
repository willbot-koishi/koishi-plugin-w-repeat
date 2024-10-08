import { h } from 'koishi'
import {} from 'koishi-plugin-w-as-forward'

export const botRepeat = (children: h[]) => <as-forward level='never'>
    { children }
</as-forward>