import {
  Activity,
  BarChart3,
  Box,
  Cog,
  Database,
  FileUp,
  FolderTree,
  Gauge,
  Home,
  Key,
  KeyRound,
  Layers,
  Megaphone,
  Puzzle,
  Server,
  Shield,
  SlidersHorizontal,
  Users,
  Wallet,
  Zap,
  type LucideIcon,
} from 'lucide-vue-next'
import { i18nText, type TextValue } from '@/i18n'

export interface NavigationItemDescriptor {
  name: TextValue
  href: string
  icon: LucideIcon
  description?: TextValue
}

export interface NavigationGroupDescriptor {
  title?: TextValue
  items: NavigationItemDescriptor[]
}

export const userNavigationGroups: NavigationGroupDescriptor[] = [
  {
    title: i18nText('layout.nav.groups.overview', '概览'),
    items: [
      { name: i18nText('layout.nav.items.dashboard', '仪表盘'), href: '/dashboard', icon: Home },
      { name: i18nText('layout.nav.items.healthMonitor', '健康监控'), href: '/dashboard/endpoint-status', icon: Activity },
    ],
  },
  {
    title: i18nText('layout.nav.groups.resources', '资源'),
    items: [
      { name: i18nText('layout.nav.items.modelCatalog', '模型目录'), href: '/dashboard/models', icon: Box },
      { name: i18nText('layout.nav.items.apiKeys', 'API 密钥'), href: '/dashboard/api-keys', icon: Key },
    ],
  },
  {
    title: i18nText('layout.nav.groups.account', '账户'),
    items: [
      { name: i18nText('layout.nav.items.walletCenter', '钱包中心'), href: '/dashboard/wallet', icon: Wallet },
      { name: i18nText('layout.nav.items.usageRecords', '使用记录'), href: '/dashboard/usage', icon: Activity },
      { name: i18nText('layout.nav.items.asyncTasks', '异步任务'), href: '/dashboard/async-tasks', icon: Zap },
    ],
  },
]

const adminSystemPrefixItems: NavigationItemDescriptor[] = [
  { name: i18nText('layout.nav.items.announcementManagement', '公告管理'), href: '/admin/announcements', icon: Megaphone },
  { name: i18nText('layout.nav.items.cacheMonitoring', '缓存监控'), href: '/admin/cache-monitoring', icon: Gauge },
]

const adminSystemSuffixItems: NavigationItemDescriptor[] = [
  { name: i18nText('layout.nav.items.moduleManagement', '模块管理'), href: '/admin/modules', icon: Puzzle },
  { name: i18nText('layout.nav.items.systemSettings', '系统设置'), href: '/admin/system', icon: Cog },
]

export const moduleNavigationIconMap: Record<string, LucideIcon> = {
  FileUp,
  Key,
  KeyRound,
  Puzzle,
  Server,
  Shield,
  SlidersHorizontal,
}

export function createModuleNavigationItem(input: {
  displayName: string
  href: string
  iconName?: string | null
}): NavigationItemDescriptor {
  return {
    name: input.displayName,
    href: input.href,
    icon: moduleNavigationIconMap[input.iconName || ''] || Puzzle,
  }
}

export function createAdminNavigationGroups(moduleItems: NavigationItemDescriptor[]): NavigationGroupDescriptor[] {
  return [
    {
      title: i18nText('layout.nav.groups.overview', '概览'),
      items: [
        { name: i18nText('layout.nav.items.dashboard', '仪表盘'), href: '/admin/dashboard', icon: Home },
        { name: i18nText('layout.nav.items.healthMonitor', '健康监控'), href: '/admin/health-monitor', icon: Activity },
        { name: i18nText('layout.nav.items.userStats', '用户统计'), href: '/admin/user-stats', icon: BarChart3 },
        { name: i18nText('layout.nav.items.costAnalysis', '成本分析'), href: '/admin/cost-analysis', icon: Gauge },
        { name: i18nText('layout.nav.items.performanceAnalysis', '性能分析'), href: '/admin/performance-analysis', icon: Activity },
      ],
    },
    {
      title: i18nText('layout.nav.groups.management', '管理'),
      items: [
        { name: i18nText('layout.nav.items.users', '用户管理'), href: '/admin/users', icon: Users },
        { name: i18nText('layout.nav.items.providers', '提供商'), href: '/admin/providers', icon: FolderTree },
        { name: i18nText('layout.nav.items.modelManagement', '模型管理'), href: '/admin/models', icon: Layers },
        { name: i18nText('layout.nav.items.poolManagement', '号池管理'), href: '/admin/pool', icon: Database },
        { name: i18nText('layout.nav.items.standaloneKeys', '独立密钥'), href: '/admin/keys', icon: Key },
        { name: i18nText('layout.nav.items.walletsManagement', '钱包管理'), href: '/admin/wallets', icon: Wallet },
        { name: i18nText('layout.nav.items.asyncTasks', '异步任务'), href: '/admin/async-tasks', icon: Zap },
        { name: i18nText('layout.nav.items.usageRecords', '使用记录'), href: '/admin/usage', icon: BarChart3 },
      ],
    },
    {
      title: i18nText('layout.nav.groups.system', '系统'),
      items: [
        ...adminSystemPrefixItems,
        ...moduleItems,
        ...adminSystemSuffixItems,
      ],
    },
  ]
}
