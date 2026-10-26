import { useEffect, useRef, useState } from 'react'
import ConfirmModal from './components/ConfirmModal'
import { TraceResults, TraceToolbar, useTraceRun } from './components/TracePanel'
import ConnectionDrawer from './components/ConnectionDrawer'
import ErrorModal from './components/ErrorModal'
import TopBar, { type AppTab } from './components/TopBar'
import TopicsSidebar from './components/TopicsSidebar'
import ConsumePanel, { ConsumeToolbar } from './components/panels/ConsumePanel'
import TailPanel, { TailToolbar } from './components/panels/TailPanel'
import ProducePanel from './components/panels/ProducePanel'
import GroupsPanel, { type GroupsMode } from './components/panels/GroupsPanel'
import SchemasPanel from './components/panels/SchemasPanel'
import LogsPanel, { useBackendDebugSync } from './components/panels/LogsPanel'
import SettingsPanel from './components/panels/SettingsPanel'
import { useAppLogs } from './hooks/useAppLogs'
import { useKafdeskApp } from './hooks/useKafdeskApp'
import { useLocale } from './hooks/useLocale'
import {
  collectKafdeskStorage,
  getFaceOwner,
  isFaceBrowser,
  setFaceOwner,
  thisSideHasControl,
} from './lib/faceControl'
import type { WebFaceStatus } from './lib/kafdeskApi'
import { useGlass } from './hooks/useGlass'
import { useTheme } from './hooks/useTheme'
import { detectUpdateOs } from './lib/updateOs'
import { installWindowStatePersistence, restoreWindowState } from './lib/windowState'
import type { MsgKey } from './i18n'
import {
  addTopicToSet,
  createTopicSet,
  deleteTopicSet,
  loadTopicSets,
  persistTopicSets,
  removeTopicFromSet,
  type TopicSet,
} from './lib/topicSets'

function isOverlayTab(tab: AppTab) {
  return tab === 'settings' || tab === 'logs'
}

export default function App() {
  const { theme, toggleTheme } = useTheme()
  useGlass()
  const { tr, toggleLocale } = useLocale()
  const { logs, setLogs, pushLog } = useAppLogs()
  const [backendDebug, setBackendDebug] = useBackendDebugSync(pushLog)
  const a = useKafdeskApp(pushLog)
  const [updatePromptDismissed, setUpdatePromptDismissed] = useState(false)
  const [groupsMode, setGroupsMode] = useState<GroupsMode>('topic')
  const [topicSets, setTopicSets] = useState<TopicSet[]>([])
  const [activeSetId, setActiveSetId] = useState<string | null>(null)
  const [traceOpen, setTraceOpen] = useState(false)
  const [traceNeedle, setTraceNeedle] = useState('')
  const [face, setFace] = useState<WebFaceStatus>(() => ({
    running: isFaceBrowser(),
    url: null,
    owner: getFaceOwner(),
  }))

  const faceGen = useRef(0)

  useEffect(() => {
    let alive = true
    const tick = () => {
      const gen = faceGen.current
      void window.kafdesk
        ?.webFaceStatus?.()
        .then((s) => {
          if (!alive || faceGen.current !== gen) return
          setFace(s)
          setFaceOwner(s.owner)
        })
        .catch(() => {})
    }
    tick()
    const id = window.setInterval(tick, 1000)
    return () => {
      alive = false
      window.clearInterval(id)
    }
  }, [])

  const hadControl = useRef(thisSideHasControl())
  useEffect(() => {
    const has = thisSideHasControl()
    if (has && !hadControl.current) void a.syncLiveSession()
    hadControl.current = has
  }, [face.owner]) // eslint-disable-line react-hooks/exhaustive-deps

  async function switchFace(owner: 'desktop' | 'web') {
    const gen = ++faceGen.current
    if (owner === 'web' && !isFaceBrowser()) {
      await window.kafdesk.webFacePutClientState(JSON.stringify(collectKafdeskStorage()))
    }
    const next = await window.kafdesk.webFaceSetOwner(owner, owner === 'web' && !isFaceBrowser())
    if (faceGen.current !== gen) return
    setFace(next)
    setFaceOwner(next.owner)
  }

  useEffect(() => {
    setTopicSets(loadTopicSets(a.activeId))
    setActiveSetId(null)
  }, [a.activeId])

  function saveSets(next: TopicSet[]) {
    setTopicSets(next)
    persistTopicSets(a.activeId, next)
  }

  function openTrace(needle?: string) {
    if (needle) setTraceNeedle(needle)
    setTraceOpen(true)
    if (a.tab !== 'consume') a.setTab('consume')
    if (!activeSetId && topicSets[0]) setActiveSetId(topicSets[0].id)
  }

  const trace = useTraceRun({
    tr,
    open: traceOpen,
    initialNeedle: traceNeedle,
    sets: topicSets,
    activeSetId,
  })
  const tabBeforeOverlay = useRef<AppTab>('consume')

  useEffect(() => {
    void restoreWindowState().then(() => installWindowStatePersistence())
  }, [])

  useEffect(() => {
    if (a.updateStatus.state !== 'downloaded') setUpdatePromptDismissed(false)
  }, [a.updateStatus.state])

  if (!a.config) return null

  const showUpdatePrompt =
    a.updateStatus.state === 'downloaded' && !updatePromptDismissed

  const updateOs = detectUpdateOs()
  const updateDetailKey: MsgKey =
    updateOs === 'mac'
      ? 'updateReadyDetailMac'
      : updateOs === 'win'
        ? 'updateReadyDetailWin'
        : 'updateReadyDetailLinux'

  const hasControl = isFaceBrowser() ? face.owner === 'web' : face.owner === 'desktop'

  return (
    <div className={`app ${a.sidebarOpen ? 'drawer-open' : ''} ${hasControl ? '' : 'face-locked'}`}>
      {!hasControl && (
        <div className="face-lock-banner" role="status">
          {isFaceBrowser() ? tr('faceLockedDesktop') : tr('faceLockedWeb')}
        </div>
      )}
      {a.error && (
        <ErrorModal
          title={tr('connectErrorTitle')}
          message={a.error}
          closeLabel={tr('close')}
          onClose={a.clearError}
        />
      )}
      {showUpdatePrompt && (
        <ConfirmModal
          title={tr('updateReadyTitle')}
          message={tr('updateReadyMessage', {
            version: a.updateStatus.version ?? '',
          })}
          detail={tr(updateDetailKey)}
          confirmLabel={tr('updateOpenInstall')}
          cancelLabel={tr('updateLater')}
          langSwitchLabel={tr('langSwitch')}
          onLangSwitch={toggleLocale}
          onConfirm={() => void a.openDownloadedUpdate()}
          onCancel={() => setUpdatePromptDismissed(true)}
        />
      )}
      <ConnectionDrawer
        tr={tr}
        sidebarOpen={a.sidebarOpen}
        setSidebarOpen={a.setSidebarOpen}
        connections={a.connections}
        activeId={a.activeId}
        pendingDeleteId={a.pendingDeleteId}
        config={a.config}
        authMethod={a.authMethod}
        brokerHost={a.brokerHost}
        setBrokerHost={a.setBrokerHost}
        brokerPort={a.brokerPort}
        setBrokerPort={a.setBrokerPort}
        connected={a.connected}
        srStatus={a.srStatus}
        busy={a.busy}
        pingInfo={a.pingInfo}
        error={a.error}
        detailsRef={a.detailsRef}
        onAddConnection={a.addConnection}
        onSelectConnection={a.selectConnection}
        onEditConnection={a.editConnection}
        onReorderConnections={a.reorderConnections}
        onDuplicateConnection={a.duplicateConnection}
        onRequestDeleteConnection={a.requestDeleteConnection}
        onCancelDeleteConnection={a.cancelDeleteConnection}
        onConfirmDeleteConnection={a.confirmDeleteConnection}
        onUpdateConfig={a.updateConfig}
        onSaveConnectionDetails={a.saveConnectionDetails}
        onConnect={a.connect}
        onReconnect={a.reconnect}
        onDisconnect={a.disconnect}
        onPing={a.ping}
        lockNote={hasControl ? undefined : isFaceBrowser() ? tr('faceLockedDesktop') : tr('faceLockedWeb')}
      />

      <main className="main">
        <TopBar
          tr={tr}
          theme={theme}
          toggleTheme={toggleTheme}
          toggleLocale={toggleLocale}
          setSidebarOpen={a.setSidebarOpen}
          onToggleSettings={() => {
            if (a.tab === 'settings') {
              a.setTab(tabBeforeOverlay.current)
            } else {
              if (!isOverlayTab(a.tab)) tabBeforeOverlay.current = a.tab
              a.setTab('settings')
            }
          }}
          settingsActive={a.tab === 'settings'}
          onToggleLogs={() => {
            if (a.tab === 'logs') {
              a.setTab(tabBeforeOverlay.current)
              // Keep a short trail after leaving Logs (RAM).
              setLogs((prev) => prev.slice(0, 40))
            } else {
              if (!isOverlayTab(a.tab)) tabBeforeOverlay.current = a.tab
              a.setTab('logs')
            }
          }}
          logsActive={a.tab === 'logs'}
          connections={a.connections}
          activeId={a.activeId}
          onSelectConnection={a.selectConnection}
          config={a.config}
          connected={a.connected}
          srStatus={a.srStatus}
          authSession={a.authSession}
          busy={a.busy}
          onConnect={a.connect}
          onReconnect={a.reconnect}
          tab={a.tab}
          onTabChange={(next) => {
            a.setTab(next)
            if (next === 'groups' && a.connected) {
              a.clearError()
              if (!a.groupSnapshot) void a.loadGroups()
              else if (Date.now() - a.groupSnapshot.updatedAt > 30_000) {
                void a.refreshGroupSnapshot(true)
              }
            }
            if (next === 'schemas' && a.connected && a.hasRegistry && a.subjects.length === 0) {
              void a.refreshSubjects()
            }
          }}
        />

        {a.tab === 'consume' && (
          <ConsumeToolbar
            tr={tr}
            selectedTopic={a.selectedTopic}
            topicDetails={a.topicDetails}
            search={a.search}
            setSearch={a.setSearch}
            searchScope={a.searchScope}
            setSearchScope={a.setSearchScope}
            search2={a.search2}
            setSearch2={a.setSearch2}
            searchScope2={a.searchScope2}
            setSearchScope2={a.setSearchScope2}
            dualSearch={a.dualSearch}
            onOpenDualSearch={a.openDualSearch}
            onCloseDualSearch={a.closeDualSearch}
            searchHitsOn={a.searchHitsOn}
            onSearchHitsSlot={a.setSearchHitsOnSlot}
            headerKey={a.headerKey}
            setHeaderKey={a.setHeaderKey}
            headerValue={a.headerValue}
            setHeaderValue={a.setHeaderValue}
            timeFrom={a.timeFrom}
            setTimeFrom={a.setTimeFrom}
            timeTo={a.timeTo}
            setTimeTo={a.setTimeTo}
            savedQueries={a.savedQueries}
            onSaveQuery={a.saveCurrentQuery}
            onApplyQuery={a.applySavedQuery}
            onDeleteQuery={a.deleteSavedQuery}
            partitionFilter={a.partitionFilter}
            setPartitionFilter={a.setPartitionFilter}
            startFrom={a.startFrom}
            setStartFrom={a.setStartFrom}
            startOffset={a.startOffset}
            setStartOffset={a.setStartOffset}
            limit={a.limit}
            setLimit={a.setLimit}
            onResetBar={a.resetFilterAndSeek}
            busy={a.loadingMessages}
            onLoadMessages={a.loadMessages}
            traceOpen={traceOpen}
            onOpenTrace={() => openTrace()}
            traceBar={
              <TraceToolbar
                tr={tr}
                connected={a.connected}
                sets={topicSets}
                active={trace.active}
                needle={trace.needle}
                setNeedle={trace.setNeedle}
                windowId={trace.windowId}
                chooseWindow={trace.chooseWindow}
                scanValues={trace.scanValues}
                chooseBodies={trace.chooseBodies}
                running={trace.running}
                onRun={() => void trace.run(trace.needle, trace.scanValues)}
                onStop={trace.stop}
                onClose={() => setTraceOpen(false)}
                onActiveSetId={setActiveSetId}
                onCreateSet={(name, topic) => {
                  const next = createTopicSet(topicSets, name, topic)
                  if (next === topicSets) return
                  saveSets(next)
                  setActiveSetId(next[next.length - 1].id)
                }}
                onDeleteSet={(setId) => {
                  const next = deleteTopicSet(topicSets, setId)
                  saveSets(next)
                  if (activeSetId === setId) setActiveSetId(next[0]?.id ?? null)
                }}
              />
            }
          />
        )}
        {a.tab === 'tail' && (
          <TailToolbar
            tr={tr}
            selectedTopic={a.selectedTopic}
            topicDetails={a.topicDetails}
            search={a.search}
            setSearch={a.setSearch}
            searchScope={a.searchScope}
            setSearchScope={a.setSearchScope}
            search2={a.search2}
            setSearch2={a.setSearch2}
            searchScope2={a.searchScope2}
            setSearchScope2={a.setSearchScope2}
            dualSearch={a.dualSearch}
            onOpenDualSearch={a.openDualSearch}
            onCloseDualSearch={a.closeDualSearch}
            searchHitsOn={a.searchHitsOn}
            onSearchHitsSlot={a.setSearchHitsOnSlot}
            partitionFilter={a.partitionFilter}
            setPartitionFilter={a.setPartitionFilter}
          />
        )}

        {a.tab === 'logs' ? (
          <LogsPanel
            tr={tr}
            logs={logs}
            onClear={() => setLogs([])}
            backendDebug={backendDebug}
            onBackendDebugChange={setBackendDebug}
          />
        ) : a.tab === 'settings' ? (
          <SettingsPanel
            tr={tr}
            updateStatus={a.updateStatus}
            updateAuto={a.updateAuto}
            onSetAutoUpdate={a.setAutoUpdateEnabled}
            onCheckUpdates={a.checkForUpdates}
            onOpenReleases={() => void a.openReleases()}
            face={face}
            onSwitchFace={switchFace}
          />
        ) : (
          <div
            className={`content ${a.tab === 'produce' ? 'content-produce' : ''} ${a.tab === 'groups' ? 'content-groups' : ''} ${a.tab === 'schemas' ? 'content-schemas' : ''}`}
          >
            {a.tab !== 'schemas' && (
              <TopicsSidebar
                tr={tr}
                connected={a.connected}
                topics={a.topics}
                topicQuery={a.topicQuery}
                setTopicQuery={a.setTopicQuery}
                selectedTopic={a.selectedTopic}
                onSelectTopic={a.selectTopic}
                pinnedTopics={a.pinnedTopics}
                onTogglePin={a.togglePinTopic}
                subjects={a.subjects}
                hasRegistry={a.hasRegistry}
                onOpenSubjectInSchemas={a.openSubjectInSchemas}
                topicLag={a.groupSnapshot?.topics}
                showLagFilter={a.tab === 'consume' || a.tab === 'tail' || a.tab === 'groups'}
                topicSets={topicSets}
                onActiveSetId={setActiveSetId}
                pickingSet={a.tab === 'consume' && traceOpen && !!trace.active}
                setTopics={a.tab === 'consume' && traceOpen && trace.active ? trace.active.topics : []}
                onToggleSetTopic={(name) => {
                  const set = trace.active
                  if (!set) return
                  saveSets(
                    set.topics.includes(name)
                      ? removeTopicFromSet(topicSets, set.id, name)
                      : addTopicToSet(topicSets, set.id, name),
                  )
                }}
                onCreateSet={(name, topic) => {
                  const next = createTopicSet(topicSets, name, topic)
                  if (next === topicSets) return
                  saveSets(next)
                  setActiveSetId(next[next.length - 1].id)
                }}
                onAddTopicToSet={(setId, topic) => saveSets(addTopicToSet(topicSets, setId, topic))}
              />
            )}

            {a.tab === 'consume' && (
              <ConsumePanel
                tr={tr}
                topicDetails={a.topicDetails}
                topicGroupLags={a.topicGroupLags}
                messages={a.filteredMessages}
                windowMessageCount={a.windowMessageCount}
                selectedTopic={a.selectedTopic}
                busy={a.loadingMessages}
                startFrom={a.startFrom}
                diffMessages={a.diffMessages}
                onToggleDiff={a.toggleDiffMessage}
                onClearDiff={a.clearDiff}
                onRepublish={a.republishMessage}
                onTraceKey={(key) => openTrace(key)}
                traceView={
                  traceOpen ? (
                    <TraceResults
                      tr={tr}
                      connected={a.connected}
                      active={trace.active}
                      clusterTopics={a.topics.map((t) => t.name)}
                      onRemoveTopic={(setId, topic) => saveSets(removeTopicFromSet(topicSets, setId, topic))}
                      running={trace.running}
                      progress={trace.progress}
                      result={trace.result}
                      error={trace.error}
                      onOpenHit={(hit) => {
                        setTraceOpen(false)
                        void a.openTraceHit(hit, trace.needle)
                      }}
                    />
                  ) : undefined
                }
                searchClauses={a.searchClauses}
                searchHitsOn={a.searchHitsOn}
                onOpenConsumerGroup={(groupId) => {
                  a.selectGroup(groupId)
                  setGroupsMode('groups')
                  a.setTab('groups')
                }}
              />
            )}
            {a.tab === 'produce' && (
              <ProducePanel
                tr={tr}
                selectedTopic={a.selectedTopic}
                produceFormat={a.produceFormat}
                setProduceFormat={a.setProduceFormat}
                subjects={a.subjects}
                subject={a.subject}
                onSubjectChange={a.onSubjectChange}
                onAutoApplyTopicSubject={a.autoApplyTopicSubject}
                hasRegistry={a.hasRegistry}
                schemaInfo={a.schemaInfo}
                localSchemaName={a.localSchemaName}
                topicDetails={a.topicDetails}
                produceKey={a.produceKey}
                setProduceKey={a.setProduceKey}
                produceHeaderRows={a.produceHeaderRows}
                setProduceHeaderRows={a.setProduceHeaderRows}
                onAddProduceHeaderRow={a.addProduceHeaderRow}
                producePartition={a.producePartition}
                setProducePartition={a.setProducePartition}
                produceValue={a.produceValue}
                setProduceValue={a.setProduceValue}
                busy={a.busy}
                onSendMessage={a.sendMessage}
                onBulkSend={a.sendBulk}
                onGenerateFromSchema={a.generateFromSchema}
                onLoadAvsc={a.loadAvscFile}
                produceResult={a.produceResult}
                error={a.error}
                templates={a.produceTemplates}
                onSaveTemplate={a.saveProduceTemplate}
                onApplyTemplate={a.applyProduceTemplate}
                onDeleteTemplate={a.deleteProduceTemplate}
              />
            )}
            {a.tab === 'tail' && (
              <TailPanel
                tr={tr}
                tailing={a.tailing}
                busy={a.busy}
                tailMessages={a.tailMessages}
                tailBufferCount={a.tailBufferCount}
                tailDropped={a.tailDropped}
                selectedTopic={a.selectedTopic}
                searchClauses={a.searchClauses}
                searchHitsOn={a.searchHitsOn}
                onStartTail={a.startTail}
                onStopTail={a.stopTail}
              />
            )}
            {a.tab === 'groups' && (
              <GroupsPanel
                tr={tr}
                connected={a.connected}
                busy={a.busy}
                filteredGroups={a.filteredGroups}
                groupQuery={a.groupQuery}
                setGroupQuery={a.setGroupQuery}
                selectedGroup={a.selectedGroup}
                onSelectGroup={a.selectGroup}
                onLoadGroups={a.loadGroups}
                selectedTopic={a.selectedTopic}
                movePartition={a.movePartition}
                setMovePartition={a.setMovePartition}
                moveOffset={a.moveOffset}
                setMoveOffset={a.setMoveOffset}
                onMoveGroupOffset={a.moveGroupOffset}
                onResetGroup={a.resetGroup}
                groupOffsets={a.groupOffsets}
                groupDetail={a.groupDetail}
                mode={groupsMode}
                setMode={setGroupsMode}
                onDescribeGroup={(groupId) => window.kafdesk.describeConsumerGroup(groupId)}
                snapshot={a.groupSnapshot}
                topicConsumers={a.topicConsumers}
                snapshotLoading={a.snapshotLoading}
                lagAutoRefresh={a.lagAutoRefresh}
                setLagAutoRefresh={a.setLagAutoRefresh}
                onRefreshSnapshot={() => void a.refreshGroupSnapshot(false)}
                onOpenTopicInConsume={(topic) => {
                  void a.selectTopic(topic)
                  a.setTab('consume')
                }}
              />
            )}
            {a.tab === 'schemas' && (
              <SchemasPanel
                tr={tr}
                connected={a.connected}
                hasRegistry={a.hasRegistry}
                subjects={a.subjects}
                busy={a.busy}
                onRefreshSubjects={a.refreshSubjects}
                onUseForProduce={a.useSubjectForProduce}
                focusSubject={a.schemasFocusSubject}
                onFocusSubjectConsumed={a.clearSchemasFocusSubject}
              />
            )}
          </div>
        )}
      </main>
    </div>
  )
}
