import type {
  ConsumerGroupInfo,
  GroupOffsetEntry,
  LagSummaryResult,
} from '../../types/kafdesk'
import type { useLocale } from '../../hooks/useLocale'

type Tr = ReturnType<typeof useLocale>['tr']

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
  lagSummary,
  lagAutoRefresh,
  setLagAutoRefresh,
  onLoadLagSummary,
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
  lagSummary: LagSummaryResult | null
  lagAutoRefresh: boolean
  setLagAutoRefresh: (v: boolean) => void
  onLoadLagSummary: () => void
}) {
  const maxGroupLag = lagSummary?.groups.reduce((m, g) => Math.max(m, g.totalLag), 0) ?? 0
  const groupsWithLag = lagSummary?.groups.filter((g) => g.totalLag > 0).length ?? 0
  const topLag = (lagSummary?.groups ?? []).filter((g) => g.totalLag > 0).slice(0, 8)

  return (
    <section className="messages groups-pane">
      <div className="panel lag-summary">
        <div className="row" style={{ justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h2 style={{ margin: 0 }}>{tr('lagMonitoring')}</h2>
          <div className="row" style={{ gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            <label className="status" style={{ gap: 6 }}>
              <input
                type="checkbox"
                checked={lagAutoRefresh}
                onChange={(e) => setLagAutoRefresh(e.target.checked)}
                disabled={!connected}
              />
              {tr('lagAutoRefresh')}
            </label>
            <button type="button" disabled={!connected || busy} onClick={onLoadLagSummary}>
              {tr('refresh')}
            </button>
            {lagSummary && (
              <span className="status">
                {tr('lagUpdated', {
                  time: new Date(lagSummary.updatedAt).toLocaleTimeString(),
                })}
              </span>
            )}
          </div>
        </div>

        <div className="lag-stats">
          <div className="lag-stat">
            <strong>{lagSummary?.totalLag ?? '—'}</strong>
            <span>{tr('lagTotal')}</span>
          </div>
          <div className="lag-stat">
            <strong>{lagSummary ? maxGroupLag : '—'}</strong>
            <span>{tr('lagMaxGroup')}</span>
          </div>
          <div className="lag-stat">
            <strong>{lagSummary ? groupsWithLag : '—'}</strong>
            <span>{tr('lagGroupsWithLag')}</span>
          </div>
          <div className="lag-stat">
            <strong>{lagSummary?.groups.length ?? filteredGroups.length}</strong>
            <span>{tr('groupsCount')}</span>
          </div>
        </div>

        {topLag.length > 0 && (
          <>
            <div className="status" style={{ marginBottom: 6 }}>
              {tr('lagPerGroup')}
              {selectedTopic ? ` · ${selectedTopic}` : ''}
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
                    {g.worstTopic
                      ? ` · ${g.worstTopic} ${tr('lagWorst', { partition: g.worstPartition ?? '?' })}`
                      : ''}
                  </span>
                  <span className="lag-bar-track">
                    <span
                      className="lag-bar-fill"
                      style={{
                        display: 'block',
                        width: `${maxGroupLag ? Math.max(3, (g.totalLag / maxGroupLag) * 100) : 0}%`,
                      }}
                    />
                  </span>
                  <strong className="lag-badge">{g.totalLag}</strong>
                </button>
              ))}
            </div>
          </>
        )}
        {lagSummary && topLag.length === 0 && <div className="empty">Lag = 0</div>}
        {!lagSummary && <div className="empty">{connected ? tr('refresh') : tr('selectGroup')}</div>}
      </div>

      <div className="groups-layout">
        <div className="groups-list drag-scroll">
          <div className="field">
            <label>{tr('consumerGroups', { n: filteredGroups.length })}</label>
            <input
              value={groupQuery}
              onChange={(e) => setGroupQuery(e.target.value)}
              placeholder={tr('filterGroups')}
              disabled={!connected}
            />
          </div>
          <button disabled={!connected || busy} onClick={onLoadGroups}>
            {tr('refresh')}
          </button>
          {filteredGroups.map((g) => (
            <button
              key={g.groupId}
              className={`topic-item ${selectedGroup === g.groupId ? 'active' : ''}`}
              onClick={() => onSelectGroup(g.groupId)}
            >
              {g.groupId}
              <small>
                {g.state || '—'}
                {g.members != null ? ` · ${tr('members', { n: g.members })}` : ''}
                {g.totalLag != null ? (
                  <>
                    {' · '}
                    <span className={g.totalLag > 0 ? 'lag-badge' : undefined}>
                      lag {g.totalLag}
                    </span>
                  </>
                ) : null}
              </small>
            </button>
          ))}
        </div>
        <div className="groups-detail drag-scroll">
          {!selectedGroup && <div className="empty">{tr('selectGroup')}</div>}
          {selectedGroup && (
            <>
              <div className="panel" style={{ marginBottom: '0.9rem' }}>
                <h2>{selectedGroup}</h2>
                <div className="status" style={{ marginBottom: 10 }}>
                  {tr('moveOffsets')}{' '}
                  <strong>{selectedTopic || tr('selectTopicForOffsets')}</strong>
                </div>
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
                    disabled={!selectedTopic || busy}
                    onClick={onMoveGroupOffset}
                    style={{ alignSelf: 'end' }}
                  >
                    {tr('setOffset')}
                  </button>
                  <button
                    disabled={!selectedTopic || busy}
                    onClick={() => onResetGroup(true)}
                    style={{ alignSelf: 'end' }}
                  >
                    {tr('resetEarliest')}
                  </button>
                  <button
                    disabled={!selectedTopic || busy}
                    onClick={() => onResetGroup(false)}
                    style={{ alignSelf: 'end' }}
                  >
                    {tr('resetLatest')}
                  </button>
                </div>
              </div>
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
                  {groupOffsets.map((o) => (
                    <tr
                      key={`${o.topic}-${o.partition}`}
                      className={selectedTopic === o.topic ? 'row-active' : undefined}
                      onClick={() => {
                        setMovePartition(String(o.partition))
                        setMoveOffset(o.offset)
                      }}
                    >
                      <td>{o.topic}</td>
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
    </section>
  )
}
