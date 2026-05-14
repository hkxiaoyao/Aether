import { Mail, Shield, AlertTriangle } from 'lucide-vue-next'
import type { LucideIcon } from 'lucide-vue-next'
import { i18nText, type TextValue } from '@/i18n'

export interface BuiltinTool {
  name: TextValue
  description: TextValue
  href: string
  icon: LucideIcon
}

export const BUILTIN_TOOLS: BuiltinTool[] = [
  {
    name: i18nText('builtinTools.email.name', '邮件配置'),
    description: i18nText('builtinTools.email.description', '配置 SMTP 邮件服务，管理邮件模板和发送设置'),
    href: '/admin/email',
    icon: Mail,
  },
  {
    name: i18nText('builtinTools.ipSecurity.name', 'IP 安全'),
    description: i18nText('builtinTools.ipSecurity.description', '管理 IP 黑白名单，控制系统访问权限'),
    href: '/admin/ip-security',
    icon: Shield,
  },
  {
    name: i18nText('builtinTools.auditLogs.name', '审计日志'),
    description: i18nText('builtinTools.auditLogs.description', '查看系统操作日志，追踪安全事件与变更记录'),
    href: '/admin/audit-logs',
    icon: AlertTriangle,
  },
]

/** href → display name mapping for breadcrumbs */
export const BUILTIN_TOOL_BREADCRUMBS: Record<string, TextValue> = Object.fromEntries(
  BUILTIN_TOOLS.map(t => [t.href, t.name])
)
