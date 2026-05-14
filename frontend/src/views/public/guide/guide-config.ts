import type { Component } from 'vue'
import {
  Rocket,
  Network,
  BookOpen,
  Target,
  Settings,
  Blocks,
  HelpCircle
} from 'lucide-vue-next'
import { i18nText, type TextValue } from '@/i18n'

// 导航配置
export interface GuideNavItem {
  id: string
  name: TextValue
  path: string
  icon: Component
  description?: TextValue
  subItems?: { name: TextValue; hash: string }[]
}

export const guideNavItems: GuideNavItem[] = [
  {
    id: 'overview',
    name: i18nText('guide.nav.overview', '快速开始'),
    path: '/guide',
    icon: Rocket,
    description: i18nText('guide.nav.overviewDescription', '部署后的配置指南'),
    subItems: [
      { name: i18nText('guide.nav.production', '部署'), hash: '#production' },
      { name: i18nText('guide.nav.configSteps', '配置流程'), hash: '#config-steps' },
      { name: i18nText('guide.nav.reverseProxy', '反向代理'), hash: '#reverse-proxy' },
      { name: i18nText('guide.nav.asyncTasks', '异步任务'), hash: '#async-tasks' },
      { name: i18nText('guide.nav.proxyConfig', '代理配置'), hash: '#proxy-config' }
    ]
  },
  {
    id: 'architecture',
    name: i18nText('guide.nav.architecture', '架构说明'),
    path: '/guide/architecture',
    icon: Network,
    description: i18nText('guide.nav.architectureDescription', '系统架构')
  },
  {
    id: 'concepts',
    name: i18nText('guide.nav.concepts', '相关概念'),
    path: '/guide/concepts',
    icon: BookOpen,
    description: i18nText('guide.nav.conceptsDescription', '核心概念'),
    subItems: [
      { name: i18nText('guide.nav.createModel', '创建统一模型'), hash: '#create-model' },
      { name: i18nText('guide.nav.addProvider', '添加提供商'), hash: '#add-provider' },
      { name: i18nText('guide.nav.addEndpoint', '添加端点'), hash: '#add-endpoint' },
      { name: i18nText('guide.nav.addKey', '添加密钥'), hash: '#add-key' },
      { name: i18nText('guide.nav.modelPermission', '模型权限'), hash: '#model-permission' },
      { name: i18nText('guide.nav.linkModel', '关联模型'), hash: '#link-model' },
      { name: i18nText('guide.nav.modelMapping', '模型映射'), hash: '#model-mapping' },
      { name: i18nText('guide.nav.reverseProxy', '反向代理'), hash: '#reverse-proxy' },
      { name: i18nText('guide.nav.priorityManagement', '优先级管理'), hash: '#priority-management' }
    ]
  },
  {
    id: 'strategy',
    name: i18nText('guide.nav.strategy', '关键策略'),
    path: '/guide/strategy',
    icon: Target,
    description: i18nText('guide.nav.strategyDescription', '关键策略'),
    subItems: [
      { name: i18nText('guide.nav.requestLogging', '请求体记录'), hash: '#request-logging' },
      { name: i18nText('guide.nav.scheduling', '调度模式'), hash: '#scheduling' },
      { name: i18nText('guide.nav.rateLimit', '访问限制'), hash: '#rate-limit' },
      { name: i18nText('guide.nav.payloadCleanup', '请求体清理'), hash: '#payload-cleanup' },
      { name: i18nText('guide.nav.cronTasks', '定时任务'), hash: '#cron-tasks' }
    ]
  },
  {
    id: 'advanced',
    name: i18nText('guide.nav.advanced', '高级功能'),
    path: '/guide/advanced',
    icon: Settings,
    description: i18nText('guide.nav.advancedDescription', '高级功能'),
    subItems: [
      { name: i18nText('guide.nav.formatConversion', '格式转换'), hash: '#format-conversion' },
      { name: i18nText('guide.nav.streamPolicy', '流式/非流式'), hash: '#stream-policy' },
      { name: i18nText('guide.nav.headerBodyEdit', '请求头/体编辑'), hash: '#header-body-edit' },
      { name: i18nText('guide.nav.modelMapping', '模型映射'), hash: '#model-mapping' },
      { name: i18nText('guide.nav.regexMapping', '正则映射'), hash: '#regex-mapping' },
      { name: i18nText('guide.nav.capabilities', '能力标签'), hash: '#capabilities' },
      { name: i18nText('guide.nav.balanceMonitor', '余额监控'), hash: '#balance-monitor' },
      { name: i18nText('guide.nav.configExport', '配置导入/出'), hash: '#config-export' },
      { name: i18nText('guide.nav.lockKey', '锁定用户密钥'), hash: '#lock-key' }
    ]
  },
  {
    id: 'modules',
    name: i18nText('guide.nav.modules', '模块管理'),
    path: '/guide/modules',
    icon: Blocks,
    description: i18nText('guide.nav.modulesDescription', '模块管理'),
    subItems: [
      { name: i18nText('guide.nav.managementTokens', '访问令牌'), hash: '#management-tokens' },
      { name: i18nText('guide.nav.emailConfig', '邮件配置'), hash: '#email-config' },
      { name: i18nText('guide.nav.oauthLogin', 'OAuth登录'), hash: '#oauth-login' },
      { name: i18nText('guide.nav.ldapAuth', 'LDAP认证'), hash: '#ldap-auth' }
    ]
  },
  {
    id: 'faq',
    name: i18nText('guide.nav.faq', '常见问题'),
    path: '/guide/faq',
    icon: HelpCircle,
    description: i18nText('guide.nav.faqDescription', '常见问题')
  }
]

// 样式类常量 - 使用 Literary Tech 主题
export const panelClasses = {
  card: 'literary-card rounded-2xl backdrop-blur-sm transition-all duration-300',
  cardHover: 'hover:-translate-y-1 hover:shadow-lg dark:hover:shadow-[var(--book-cloth)]/10 shadow-[var(--book-cloth)]/10',
  section: 'literary-surface-inset bg-white/40 dark:bg-black/20 backdrop-blur-md rounded-xl md:rounded-2xl p-5 md:p-8 transition-colors',
  commandPanel: 'literary-surface-elevated rounded-xl overflow-hidden shadow-sm backdrop-blur-md',
  configPanel: 'literary-surface-elevated rounded-xl overflow-hidden',
  panelHeader: 'px-4 py-3 border-b literary-border bg-[var(--color-background-soft)]/50',
  codeBody: 'p-0',
  badge: 'literary-badge bg-[var(--color-background)] rounded-full px-3 py-1.5',
  badgeBlue: 'inline-flex items-center gap-1.5 rounded-full bg-blue-500/10 dark:bg-blue-500/20 border border-blue-500/20 dark:border-blue-500/40 px-2 py-0.5 text-xs font-medium text-blue-600 dark:text-blue-400',
  badgeGreen: 'inline-flex items-center gap-1.5 rounded-full bg-green-500/10 dark:bg-green-500/20 border border-green-500/20 dark:border-green-500/40 px-2 py-0.5 text-xs font-medium text-green-600 dark:text-green-400',
  badgeYellow: 'inline-flex items-center gap-1.5 rounded-full bg-yellow-500/10 dark:bg-yellow-500/20 border border-yellow-500/20 dark:border-yellow-500/40 px-2 py-0.5 text-xs font-medium text-yellow-600 dark:text-yellow-400',
  badgePurple: 'inline-flex items-center gap-1.5 rounded-full bg-purple-500/10 dark:bg-purple-500/20 border border-purple-500/20 dark:border-purple-500/40 px-2 py-0.5 text-xs font-medium text-purple-600 dark:text-purple-400',
  iconButtonSmall: [
    'flex items-center justify-center rounded-lg border h-8 w-8',
    'literary-border',
    'bg-transparent',
    'text-[var(--color-text)]',
    'transition hover:bg-[var(--color-background-soft)]'
  ].join(' ')
} as const
