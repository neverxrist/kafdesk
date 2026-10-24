import { useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react'
import type {
  ConsumerGroupDetail,
  ConsumerGroupInfo,
  GroupOffsetEntry,
  GroupSnapshotView,
  LagTrend,
  SnapshotGroupRow,
  TopicConsumerRow,
  TopicConsumersView,
} from '../../types/kafdesk'
import type { useLocale } from '../../hooks/useLocale'
import {
  clampGroupsWidth,
  loadGroupsWidth,
  persistGroupsWidth,
} from '../../lib/groupsLayout'
import { RefreshIcon } from '../ConnActionIcons'

type Tr = ReturnType<typeof useLocale>['tr']
type GroupSort = 'name' | 'lag' | 'members'
export type GroupsMode = 'groups' | 'topic'

function normalizeState(state?: string | null) {
  const s = (state || '').replace(/["']/g, '').trim()
  if (!s) return '—'
  const lower = s.toLowerCase()
  if (lower.includes('stable')) return 'Stable'
  if (lower.includes('empty')) return 'Empty'
  if (lower.includes('dead')) return 'Dead'
  if (lower.includes('preparing')) return 'Preparing'
  if (lower.includes('completing')) return 'Completing'
  if (lower.includes('rebalance')) return 'Rebalance'
  return s.length > 16 ? `${s.slice(0, 14)}…` : s
}

function stateClass(state?: string | null) {
  const s = (state || '').toLowerCase()
  // `state-*` — bare `.empty` is the global empty-state block style.
  if (s.includes('stable')) return 'state-stable'
  if (s.includes('empty')) return 'state-empty'
  if (s.includes('dead')) return 'state-dead'
  if (s.includes('rebalance') || s.includes('preparing') || s.includes('completing')) return 'state-rebalance'
  return 'state-unknown'
}

function isEmptyGroup(g: ConsumerGroupInfo) {
  const s = (g.state || '').toLowerCase()
  return (g.members ?? 0) === 0 || s.includes('empty')
}

function groupCanReset(detail: ConsumerGroupDetail | null, g: ConsumerGroupInfo | undefined) {
  const members = detail?.memberCount ?? g?.members ?? 0
  const state = (detail?.state || g?.state || '').toLowerCase()
  if (members > 0) return false
  if (state.includes('stable') || state.includes('rebalance')) return false
  return true
}

function formatLag(n: number | null | undefined) {
  if (n == null) return '—'
  return n.toLocaleString()
}

function StateBadge({ state }: { state?: string | null }) {
  return <span className={`group-state-badge ${stateClass(state)}`}>{normalizeState(state)}</span>
}

/** Inline SVG — no chart lib. Points are lag per snapshot, oldest first. */
function LagSparkline({ points }: { points: number[] }) {
  if (points.length < 2) return null
  const w = 56
  const h = 14
  const max = Math.max(...points, 1)
  const step = w / (points.length - 1)
  const d = points
    .map((p, i) => `${(i * step).toFixed(1)},${(h - 1 - (p / max) * (h - 2)).toFixed(1)}`)
    .join(' ')
  return (
    <svg className="lag-sparkline" width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden>
      <polyline points={d} fill="none" stroke="currentColor" strokeWidth="1.3" />
    </svg>
  )
}

function TrendMarks({
  tr,
  trend,
  stalled,
  history,
}: {
  tr: Tr
  trend?: LagTrend | null
  stalled: boolean
  history?: number[]
}) {
  const arrow = trend === 'up' ? '↑' : trend === 'down' ? '↓' : trend === 'flat' ? '→' : null
  const title =
    trend === 'up' ? tr('lagTrendUp') : trend === 'down' ? tr('lagTrendDown') : tr('lagTrendFlat')
  return (
    <span className={`lag-trend${trend ? ` trend-${trend}` : ''}`}>
      {history && <LagSparkline points={history} />}
      {arrow && <span title={title}>{arrow}</span>}
      {stalled && (
        <span className="lag-stalled-badge" title={tr('lagStalledTitle')}>
          {tr('lagStalled')}
        </span>
      )}
    </span>
  )
}

export default function GroupsPanel({
  tr,
  connected,
  busy,
  filteredGroups,
  groupQuery,
  setGroupQuery,
  selectedGroup,
  onSelectGroup,
  onLoadGroups,
  selectedTopic,
  movePartition,
  setMovePartition,
  moveOffset,
  setMoveOffset,
  onMoveGroupOffset,
  onResetGroup,
  groupOffsets,
  groupDetail,
  mode,
  setMode,
  snapshot,
  topicConsumers,
  snapshotLoading,
  lagAutoRefresh,
  setLagAutoRefresh,
  onRefreshSnapshot,
  onDescribeGroup,
  onOpenTopicInConsume,
}: {
  tr: Tr
  connected: boolean
  busy: boolean
  filteredGroups: ConsumerGroupInfo[]
  groupQuery: string
  setGroupQuery: (v: string) => void
  selectedGroup: string | null
  onSelectGroup: (groupId: string) => void
  onLoadGroups: () => void
  selectedTopic: string | null
  movePartition: string
  setMovePartition: (v: string) => void
  moveOffset: string
  setMoveOffset: (v: string) => void
  onMoveGroupOffset: () => void
  onResetGroup: (earliest: boolean) => void
  groupOffsets: GroupOffsetEntry[]
  groupDetail: ConsumerGroupDetail | null
  mode: GroupsMode
  setMode: (m: GroupsMode) => void
  snapshot: GroupSnapshotView | null
  topicConsumers: TopicConsumersView | null
  snapshotLoading: boolean
  lagAutoRefresh: boolean
  setLagAutoRefresh: (v: boolean) => void
  onRefreshSnapshot: () => void
  onDescribeGroup: (groupId: string) => Promise<ConsumerGroupDetail>
  onOpenTopicInConsume: (topic: string) => void
}) {
  const [lagOnly, setLagOnly] = useState(false)
  const [stalledOnly, setStalledOnly] = useState(false)
  // Default off — clusters with hundreds of Empty groups would otherwise show an empty list.
  const [hideEmpty, setHideEmpty] = useState(false)
  const [sortBy, setSortBy] = useState<GroupSort>('name')
  const [groupsWidth, setGroupsWidth] = useState(() => loadGroupsWidth())
  const [resizing, setResizing] = useState(false)
  const resizeStart = useRef<{ x: number; w: number } | null>(null)

  // Topic picked in the sidebar → show who reads it (not on mount: keep the parent's mode).
  const prevTopic = useRef(selectedTopic)
  useEffect(() => {
    if (prevTopic.current === selectedTopic) return
    prevTopic.current = selectedTopic
    setMode(selectedTopic ? 'topic' : 'groups')
  }, [selectedTopic, setMode])

  const snapById = useMemo(() => {
    const m = new Map<string, SnapshotGroupRow>()
    for (const g of snapshot?.groups ?? []) m.set(g.groupId, g)
    return m
  }, [snapshot])

  const selectedMeta = useMemo(
    () => filteredGroups.find((g) => g.groupId === selectedGroup),
    [filteredGroups, selectedGroup],
  )
  const selectedSnap = selectedGroup ? snapById.get(selectedGroup) : undefined
  const canReset = groupCanReset(groupDetail, selectedMeta)

  const sortedOffsets = useMemo(() => {
    if (!selectedTopic) return groupOffsets
    const own = groupOffsets.filter((o) => o.topic === selectedTopic)
    return own.length ? [...own, ...groupOffsets.filter((o) => o.topic !== selectedTopic)] : groupOffsets
  }, [groupOffsets, selectedTopic])

  const visibleGroups = useMemo(() => {
    let list = filteredGroups
    if (hideEmpty) list = list.filter((g) => !isEmptyGroup(g))
    if (lagOnly) list = list.filter((g) => (g.totalLag ?? 0) > 0)
    if (stalledOnly) list = list.filter((g) => snapById.get(g.groupId)?.stalled)
    const sorted = [...list]
    sorted.sort((a, b) => {
      if (sortBy === 'lag') {
        const la = a.totalLag ?? -1
        const lb = b.totalLag ?? -1
        if (lb !== la) return lb - la
      } else if (sortBy === 'members') {
        const ma = a.members ?? 0
        const mb = b.members ?? 0
        if (mb !== ma) return mb - ma
      }
      return a.groupId.localeCompare(b.groupId)
    })
    return sorted
  }, [filteredGroups, hideEmpty, lagOnly, stalledOnly, snapById, sortBy])

  const q = groupQuery.trim().toLowerCase()
  const topicRows = useMemo(() => {
    const rows = topicConsumers?.groups ?? []
    return q ? rows.filter((r) => r.groupId.toLowerCase().includes(q)) : rows
  }, [topicConsumers, q])

  const groupStats = useMemo(() => {
    let total = 0
    let withLag = 0
    let stalled = 0
    for (const g of filteredGroups) {
      const lag = g.totalLag ?? 0
      total += lag
      if (lag > 0) withLag++
      if (snapById.get(g.groupId)?.stalled) stalled++
    }
    return { total, withLag, stalled }
  }, [filteredGroups, snapById])

  const topicStats = useMemo(() => {
    let total = 0
    let maxPartition = 0
    let active = 0
    let stalled = 0
    for (const r of topicRows) {
      total += r.lag ?? 0
      maxPartition = Math.max(maxPartition, r.maxPartitionLag)
      if (r.members > 0) active++
      if (r.stalled) stalled++
    }
    return { total, maxPartition, active, stalled }
  }, [topicRows])

  const topLag = useMemo(
    () =>
      (snapshot?.groups ?? [])
        .filter((g) => (g.totalLag ?? 0) > 0 && (!q || g.groupId.toLowerCase().includes(q)))
        .sort((a, b) => (b.totalLag ?? 0) - (a.totalLag ?? 0))
        .slice(0, 8),
    [snapshot, q],
  )
  const maxTopLag = topLag[0]?.totalLag ?? 0

  useEffect(() => {
    if (!resizing) return
    function onMove(e: PointerEvent) {
      const start = resizeStart.current
      if (!start) return
      setGroupsWidth(clampGroupsWidth(start.w + (e.clientX - start.x)))
    }
    function onUp() {
      resizeStart.current = null
      setResizing(false)
      document.body.classList.remove('groups-resizing')
      setGroupsWidth((w) => {
        persistGroupsWidth(w)
        return w
      })
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp)
    window.addEventListener('pointercancel', onUp)
    return () => {
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
      window.removeEventListener('pointercancel', onUp)
    }
  }, [resizing])

  function onResizePointerDown(e: ReactPointerEvent<HTMLDivElement>) {
    e.preventDefault()
    resizeStart.current = { x: e.clientX, w: groupsWidth }
    setResizing(true)
    document.body.classList.add('groups-resizing')
  }

  function openGroup(groupId: string) {
    setMode('groups')
    onSelectGroup(groupId)
  }

  const inTopicMode = mode === 'topic' && !!selectedTopic

  return (
    <section className="messages groups-pane">
      <div className="panel lag-summary">
        <div className="row" style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h2 style={{ margin: 0 }}>{tr('lagMonitoring')}</h2>
          <div className="row" style={{ gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            {snapshot && (
              <span className="status">
                {tr('snapshotSummary', {
                  time: new Date(snapshot.updatedAt).toLocaleTimeString(),
                  groups: snapshot.listed,
                  sec: (snapshot.durationMs / 1000).toFixed(1),
                })}
              </span>
            )}
            <label className="status" style={{ gap: 6 }}>
              <input
                type="checkbox"
                checked={lagAutoRefresh}
                onChange={(e) => setLagAutoRefresh(e.target.checked)}
                disabled={!connected}
              />
              {tr('lagAutoRefresh')}
            </label>
            <button
              type="button"
              className={`refresh-icon-btn${snapshotLoading ? ' busy' : ''}`}
              disabled={!connected || snapshotLoading}
              onClick={onRefreshSnapshot}
              title={tr('refresh')}
              aria-label={tr('refresh')}
            >
              <RefreshIcon />
            </button>
          </div>
        </div>

        <div className="groups-mode-switch" role="tablist">
          <button
            type="button"
            role="tab"
            aria-selected={!inTopicMode}
            className={!inTopicMode ? 'active' : ''}
            onClick={() => setMode('groups')}
          >
            {tr('groupsModeGroups', { n: filteredGroups.length })}
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={inTopicMode}
            className={inTopicMode ? 'active' : ''}
            disabled={!selectedTopic}
            title={!selectedTopic ? tr('groupsPickTopicHint') : selectedTopic}
            onClick={() => setMode('topic')}
          >
            {selectedTopic
              ? tr('groupsModeTopic', { topic: selectedTopic })
              : tr('groupsModeTopicNone')}
          </button>
        </div>

        {inTopicMode ? (
          <div className="lag-stats">
            <div className="lag-stat">
              <strong>{topicConsumers ? formatLag(topicStats.total) : '—'}</strong>
              <span>{tr('topicLagOnTopic')}</span>
            </div>
            <div className="lag-stat">
              <strong>
                {topicConsumers ? `${topicStats.active} / ${topicRows.length}` : '—'}
              </strong>
              <span>{tr('topicGroupsActiveAll')}</span>
            </div>
            <div className="lag-stat">
              <strong>{topicConsumers ? formatLag(topicStats.maxPartition) : '—'}</strong>
              <span>{tr('lagMaxPartition')}</span>
            </div>
            <div className="lag-stat">
              <strong>{topicConsumers ? topicStats.stalled : '—'}</strong>
              <span>{tr('lagStalledCount')}</span>
            </div>
          </div>
        ) : (
          <div className="lag-stats">
            <div className="lag-stat">
              <strong>{snapshot ? formatLag(groupStats.total) : '—'}</strong>
              <span>{tr('lagTotal')}</span>
            </div>
            <button
              type="button"
              className={`lag-stat lag-stat-btn${lagOnly ? ' active' : ''}`}
              disabled={!connected || !snapshot}
              title={tr('groupsWithLagOnlyTitle')}
              aria-pressed={lagOnly}
              onClick={() => setLagOnly((v) => !v)}
            >
              <strong>{snapshot ? groupStats.withLag : '—'}</strong>
              <span>{tr('lagGroupsWithLag')}</span>
            </button>
            <button
              type="button"
              className={`lag-stat lag-stat-btn${stalledOnly ? ' active' : ''}`}
              disabled={!connected || !snapshot}
              title={tr('lagStalledTitle')}
              aria-pressed={stalledOnly}
              onClick={() => setStalledOnly((v) => !v)}
            >
              <strong>{snapshot ? groupStats.stalled : '—'}</strong>
              <span>{tr('lagStalledCount')}</span>
            </button>
            <div className="lag-stat">
              <strong>{visibleGroups.length}</strong>
              <span>{tr('groupsCount')}</span>
            </div>
          </div>
        )}

        {snapshot?.partial && (
          <div className="status lag-partial-hint">
            {tr('snapshotPartial', { scanned: snapshot.scanned, listed: snapshot.listed })}
          </div>
        )}
        {snapshot && snapshot.failed > 0 && (
          <div className="status lag-partial-hint">{tr('snapshotFailed', { n: snapshot.failed })}</div>
        )}
        {!snapshot && connected && (
          <div className="status">{snapshotLoading ? tr('snapshotLoading') : tr('snapshotEmpty')}</div>
        )}
        {!selectedTopic && connected && snapshot && (
          <div className="status">{tr('groupsPickTopicHint')}</div>
        )}

        {!inTopicMode && topLag.length > 0 && (
          <>
            <div className="status" style={{ marginBottom: 6 }}>
              {tr('lagPerGroup')}
            </div>
            <div className="lag-bars">
              {topLag.map((g) => (
                <button
                  key={g.groupId}
                  type="button"
                  className={`lag-bar-row topic-item ${selectedGroup === g.groupId ? 'active' : ''}`}
                  onClick={() => onSelectGroup(g.groupId)}
                  title={g.groupId}
                >
                  <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {g.groupId}
                  </span>
                  <span className="lag-bar-track">
                    <span
                      className="lag-bar-fill"
                      style={{
                        display: 'block',
                        width: `${maxTopLag ? Math.max(3, ((g.totalLag ?? 0) / maxTopLag) * 100) : 0}%`,
                      }}
                    />
                  </span>
                  <strong className="lag-badge">{formatLag(g.totalLag)}</strong>
                </button>
              ))}
            </div>
          </>
        )}
      </div>

      {inTopicMode ? (
        <TopicConsumersSection
          tr={tr}
          topic={selectedTopic!}
          view={topicConsumers}
          rows={topicRows}
          hasSnapshot={!!snapshot}
          snapshotLoading={snapshotLoading}
          groupQuery={groupQuery}
          setGroupQuery={setGroupQuery}
          connected={connected}
          onOpenGroup={openGroup}
          onDescribeGroup={onDescribeGroup}
        />
      ) : (
        <div className="groups-layout">
          <div
            className={`groups-list drag-scroll${resizing ? ' is-resizing' : ''}`}
            style={{ width: groupsWidth }}
          >
            <div
              className="groups-resize"
              role="separator"
              aria-orientation="vertical"
              aria-valuenow={groupsWidth}
              title={tr('resizeGroups')}
              onPointerDown={onResizePointerDown}
            />
            <div className="filter-toolbar">
              <div className="field">
                <label>{tr('consumerGroups', { n: visibleGroups.length })}</label>
                <input
                  value={groupQuery}
                  onChange={(e) => setGroupQuery(e.target.value)}
                  placeholder={tr('filterGroups')}
                  disabled={!connected}
                  autoCapitalize="none"
                  autoCorrect="off"
                  autoComplete="off"
                  spellCheck={false}
                  lang="en"
                />
              </div>
              <button
                type="button"
                className={`refresh-icon-btn${snapshotLoading ? ' busy' : ''}`}
                disabled={!connected || snapshotLoading}
                onClick={onLoadGroups}
                title={tr('refresh')}
                aria-label={tr('refresh')}
              >
                <RefreshIcon />
              </button>
            </div>
            <div className="groups-list-filters">
              <label className="groups-lag-filter" title={tr('groupsHideEmptyTitle')}>
                <input
                  type="checkbox"
                  checked={hideEmpty}
                  onChange={(e) => setHideEmpty(e.target.checked)}
                  disabled={!connected}
                />
                {tr('groupsHideEmpty')}
              </label>
              <label className="groups-lag-filter" title={tr('groupsWithLagOnlyTitle')}>
                <input
                  type="checkbox"
                  checked={lagOnly}
                  onChange={(e) => setLagOnly(e.target.checked)}
                  disabled={!connected}
                />
                {tr('groupsWithLagOnly')}
              </label>
              <label className="groups-sort">
                <span>{tr('groupsSort')}</span>
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value as GroupSort)}
                  disabled={!connected}
                >
                  <option value="lag">{tr('groupsSortLag')}</option>
                  <option value="name">{tr('groupsSortName')}</option>
                  <option value="members">{tr('groupsSortMembers')}</option>
                </select>
              </label>
            </div>
            {visibleGroups.length === 0 && (
              <div className="empty" style={{ padding: '0.75rem 0' }}>
                {filteredGroups.length > 0 && (hideEmpty || lagOnly || stalledOnly)
                  ? tr('groupsHiddenByFilters', { n: filteredGroups.length })
                  : tr('groupsListEmpty')}
              </div>
            )}
            {visibleGroups.map((g) => {
              const snap = snapById.get(g.groupId)
              const pills = (g.topicNames ?? []).slice(0, 2)
              const morePills = (g.lagTopics ?? g.topicNames?.length ?? 0) - pills.length
              return (
                <button
                  key={g.groupId}
                  className={`topic-item groups-row ${selectedGroup === g.groupId ? 'active' : ''}`}
                  onClick={() => onSelectGroup(g.groupId)}
                >
                  <span className="groups-row-id">{g.groupId}</span>
                  <span className="groups-row-meta">
                    <StateBadge state={g.state} />
                    <span className="status">
                      {g.members != null ? tr('members', { n: g.members }) : '—'}
                    </span>
                    {g.totalLag != null && (
                      <span className={g.totalLag > 0 ? 'lag-badge' : 'status'}>
                        lag {formatLag(g.totalLag)}
                      </span>
                    )}
                    {snap?.error && (
                      <span className="lag-partial-hint" title={snap.error}>
                        {tr('groupOffsetsUnavailable')}
                      </span>
                    )}
                    {snap && <TrendMarks tr={tr} trend={snap.trend} stalled={snap.stalled} />}
                  </span>
                  {pills.length > 0 && (
                    <span className="groups-topic-pills">
                      {pills.map((t) => (
                        <span key={t} className="groups-topic-pill" title={t}>
                          {t}
                        </span>
                      ))}
                      {morePills > 0 ? (
                        <span className="groups-topic-pill more">+{morePills}</span>
                      ) : null}
                    </span>
                  )}
                </button>
              )
            })}
          </div>
          <div className="groups-detail drag-scroll">
            {!selectedGroup && <div className="empty">{tr('selectGroup')}</div>}
            {selectedGroup && (
              <>
                <div className="panel" style={{ marginBottom: '0.9rem' }}>
                  <h2 style={{ marginTop: 0 }}>{selectedGroup}</h2>
                  <div className="groups-detail-summary">
                    <StateBadge state={groupDetail?.state || selectedMeta?.state} />
                    <span className="status">
                      {tr('members', {
                        n: groupDetail?.memberCount ?? selectedMeta?.members ?? 0,
                      })}
                    </span>
                    {groupDetail?.coordinatorId != null && (
                      <span className="status">
                        {tr('groupsCoordinator', { id: groupDetail.coordinatorId })}
                      </span>
                    )}
                    {selectedMeta?.totalLag != null && (
                      <span className={selectedMeta.totalLag > 0 ? 'lag-badge' : 'status'}>
                        lag {formatLag(selectedMeta.totalLag)}
                      </span>
                    )}
                    {selectedSnap && (
                      <TrendMarks
                        tr={tr}
                        trend={selectedSnap.trend}
                        stalled={selectedSnap.stalled}
                        history={selectedSnap.history}
                      />
                    )}
                  </div>
                  {selectedSnap?.error && (
                    <div className="status lag-partial-hint" style={{ marginTop: 6 }}>
                      {selectedSnap.error}
                    </div>
                  )}
                  {(groupDetail?.topics?.length || 0) > 0 && (
                    <div className="groups-topic-pills" style={{ marginTop: 8 }}>
                      {groupDetail!.topics.map((t) => (
                        <button
                          key={t}
                          type="button"
                          className="groups-topic-pill clickable"
                          title={tr('groupsOpenInConsume')}
                          onClick={() => onOpenTopicInConsume(t)}
                        >
                          {t}
                        </button>
                      ))}
                    </div>
                  )}
                  <div className="status" style={{ margin: '10px 0' }}>
                    {tr('moveOffsets')}{' '}
                    <strong>{selectedTopic || tr('selectTopicForOffsets')}</strong>
                  </div>
                  {!canReset && (
                    <div className="status lag-partial-hint" style={{ marginBottom: 8 }}>
                      {tr('groupsResetNeedsEmpty')}
                    </div>
                  )}
                  <div className="row" style={{ gap: 8, flexWrap: 'wrap' }}>
                    <div className="field" style={{ margin: 0, width: 100 }}>
                      <label>{tr('partition')}</label>
                      <input value={movePartition} onChange={(e) => setMovePartition(e.target.value)} />
                    </div>
                    <div className="field" style={{ margin: 0, flex: 1, minWidth: 120 }}>
                      <label>{tr('offset')}</label>
                      <input value={moveOffset} onChange={(e) => setMoveOffset(e.target.value)} />
                    </div>
                    <button
                      className="primary"
                      disabled={!selectedTopic || busy || !canReset}
                      onClick={onMoveGroupOffset}
                      style={{ alignSelf: 'end' }}
                      title={!canReset ? tr('groupsResetNeedsEmpty') : undefined}
                    >
                      {tr('setOffset')}
                    </button>
                    <button
                      disabled={!selectedTopic || busy || !canReset}
                      onClick={() => onResetGroup(true)}
                      style={{ alignSelf: 'end' }}
                      title={!canReset ? tr('groupsResetNeedsEmpty') : undefined}
                    >
                      {tr('resetEarliest')}
                    </button>
                    <button
                      disabled={!selectedTopic || busy || !canReset}
                      onClick={() => onResetGroup(false)}
                      style={{ alignSelf: 'end' }}
                      title={!canReset ? tr('groupsResetNeedsEmpty') : undefined}
                    >
                      {tr('resetLatest')}
                    </button>
                  </div>
                </div>

                {groupDetail && groupDetail.members.length > 0 && (
                  <div className="panel" style={{ marginBottom: '0.9rem' }}>
                    <h3 style={{ margin: '0 0 0.5rem', fontSize: '0.85rem' }}>{tr('groupsMembers')}</h3>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>{tr('groupsMemberClient')}</th>
                          <th>{tr('groupsMemberHost')}</th>
                          <th>{tr('groupsMemberAssignment')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {groupDetail.members.map((m) => (
                          <tr key={m.memberId || m.clientId}>
                            <td title={m.memberId}>{m.clientId || m.memberId || '—'}</td>
                            <td>{m.host || '—'}</td>
                            <td>
                              {m.assignment.length === 0
                                ? '—'
                                : m.assignment
                                    .slice(0, 8)
                                    .map((a) => `${a.topic}:${a.partition}`)
                                    .join(', ') +
                                  (m.assignment.length > 8
                                    ? ` (+${m.assignment.length - 8})`
                                    : '')}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                <table className="data-table">
                  <thead>
                    <tr>
                      <th>{tr('colTopic')}</th>
                      <th>{tr('colP')}</th>
                      <th>{tr('colOffset')}</th>
                      <th>{tr('colLow')}</th>
                      <th>{tr('colHigh')}</th>
                      <th>{tr('colLag')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedOffsets.map((o) => (
                      <tr
                        key={`${o.topic}-${o.partition}`}
                        className={selectedTopic === o.topic ? 'row-active' : undefined}
                        onClick={() => {
                          setMovePartition(String(o.partition))
                          setMoveOffset(o.offset)
                        }}
                      >
                        <td>
                          <button
                            type="button"
                            className="linkish"
                            title={tr('groupsOpenInConsume')}
                            onClick={(e) => {
                              e.stopPropagation()
                              onOpenTopicInConsume(o.topic)
                            }}
                          >
                            {o.topic}
                          </button>
                        </td>
                        <td>{o.partition}</td>
                        <td>{o.offset}</td>
                        <td>{o.low ?? '—'}</td>
                        <td>{o.high ?? '—'}</td>
                        <td className={o.lag ? 'lag-badge' : undefined}>{o.lag ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {groupOffsets.length === 0 && <div className="empty">{tr('noOffsets')}</div>}
              </>
            )}
          </div>
        </div>
      )}
    </section>
  )
}

/** Who reads the topic: active groups first, inactive (no members) folded. */
function TopicConsumersSection({
  tr,
  topic,
  view,
  rows,
  hasSnapshot,
  snapshotLoading,
  groupQuery,
  setGroupQuery,
  connected,
  onOpenGroup,
  onDescribeGroup,
}: {
  tr: Tr
  topic: string
  view: TopicConsumersView | null
  rows: TopicConsumerRow[]
  hasSnapshot: boolean
  snapshotLoading: boolean
  groupQuery: string
  setGroupQuery: (v: string) => void
  connected: boolean
  onOpenGroup: (groupId: string) => void
  onDescribeGroup: (groupId: string) => Promise<ConsumerGroupDetail>
}) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const [inactiveOpen, setInactiveOpen] = useState(false)
  /** partition → "client @ host", per group; loaded lazily on expand. */
  const [owners, setOwners] = useState<Record<string, Record<number, string>>>({})

  useEffect(() => {
    setExpanded(new Set())
    setOwners({})
  }, [topic])

  const active = rows.filter((r) => r.members > 0)
  const inactive = rows.filter((r) => r.members === 0)

  function toggle(r: TopicConsumerRow) {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(r.groupId)) next.delete(r.groupId)
      else next.add(r.groupId)
      return next
    })
    if (r.members > 0 && !owners[r.groupId]) {
      void onDescribeGroup(r.groupId)
        .then((d) => {
          const map: Record<number, string> = {}
          for (const m of d.members) {
            const who = [m.clientId || m.memberId, m.host].filter(Boolean).join(' @ ')
            for (const a of m.assignment) if (a.topic === topic) map[a.partition] = who
          }
          setOwners((prev) => ({ ...prev, [r.groupId]: map }))
        })
        .catch(() => setOwners((prev) => ({ ...prev, [r.groupId]: {} })))
    }
  }

  function renderRows(list: TopicConsumerRow[]) {
    return list.map((r) => {
      const open = expanded.has(r.groupId)
      const own = owners[r.groupId]
      return [
        <tr key={r.groupId} className="topic-consumer-row" onClick={() => toggle(r)}>
          <td className="topic-consumer-caret">{open ? '▾' : '▸'}</td>
          <td>
            <button
              type="button"
              className="linkish"
              title={tr('groupsOpenGroup')}
              onClick={(e) => {
                e.stopPropagation()
                onOpenGroup(r.groupId)
              }}
            >
              {r.groupId}
            </button>
          </td>
          <td>
            <StateBadge state={r.state} />
          </td>
          <td>{r.members}</td>
          <td className={(r.lag ?? 0) > 0 ? 'lag-badge' : undefined}>{formatLag(r.lag)}</td>
          <td>{formatLag(r.maxPartitionLag)}</td>
          <td>
            <TrendMarks tr={tr} trend={r.trend} stalled={r.stalled} history={r.history} />
          </td>
        </tr>,
        open && (
          <tr key={`${r.groupId}-parts`} className="topic-consumer-parts">
            <td />
            <td colSpan={6}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>{tr('colP')}</th>
                    <th>{tr('colCommitted')}</th>
                    <th>{tr('colEnd')}</th>
                    <th>{tr('colLag')}</th>
                    <th>{tr('colMember')}</th>
                  </tr>
                </thead>
                <tbody>
                  {r.partitions.map((p) => (
                    <tr key={p.partition}>
                      <td>{p.partition}</td>
                      <td>{p.offset}</td>
                      <td>{p.high ?? '—'}</td>
                      <td className={p.lag ? 'lag-badge' : undefined}>{formatLag(p.lag)}</td>
                      <td>
                        {r.members === 0
                          ? '—'
                          : own
                            ? own[p.partition] || '—'
                            : '…'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </td>
          </tr>
        ),
      ]
    })
  }

  const head = (
    <thead>
      <tr>
        <th />
        <th>{tr('colGroup')}</th>
        <th>{tr('colState')}</th>
        <th>{tr('colMembers')}</th>
        <th>{tr('colLag')}</th>
        <th>{tr('colMaxPartition')}</th>
        <th>{tr('colTrend')}</th>
      </tr>
    </thead>
  )

  return (
    <div className="panel topic-consumers">
      <div className="filter-toolbar" style={{ marginBottom: 8 }}>
        <div className="field">
          <label>{tr('topicConsumersTitle', { topic })}</label>
          <input
            value={groupQuery}
            onChange={(e) => setGroupQuery(e.target.value)}
            placeholder={tr('filterGroups')}
            disabled={!connected}
            autoCapitalize="none"
            autoCorrect="off"
            autoComplete="off"
            spellCheck={false}
            lang="en"
          />
        </div>
      </div>
      {!hasSnapshot ? (
        <div className="empty">{snapshotLoading ? tr('snapshotLoading') : tr('snapshotEmpty')}</div>
      ) : !view ? (
        <div className="empty">{tr('snapshotLoading')}</div>
      ) : view.groups.length === 0 ? (
        <div className="empty">{tr('topicNoConsumers', { topic })}</div>
      ) : rows.length === 0 ? (
        <div className="empty">{tr('groupsListEmpty')}</div>
      ) : (
        <>
          <div className="topic-consumers-section-head">
            {tr('topicConsumersActive', { n: active.length })}
          </div>
          {active.length > 0 ? (
            <table className="data-table topic-consumers-table">
              {head}
              <tbody>{renderRows(active)}</tbody>
            </table>
          ) : (
            <div className="status">{tr('topicNoActiveConsumers')}</div>
          )}
          {inactive.length > 0 && (
            <>
              <button
                type="button"
                className="topic-fold-toggle topic-consumers-fold"
                aria-expanded={inactiveOpen}
                onClick={() => setInactiveOpen((v) => !v)}
              >
                <span>{tr('topicConsumersInactive', { n: inactive.length })}</span>
                <span className="topic-fold-chevron">{inactiveOpen ? '▴' : '▾'}</span>
              </button>
              {inactiveOpen && (
                <table className="data-table topic-consumers-table">
                  {head}
                  <tbody>{renderRows(inactive)}</tbody>
                </table>
              )}
            </>
          )}
        </>
      )}
    </div>
  )
}
