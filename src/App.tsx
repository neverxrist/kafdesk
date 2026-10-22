import { useEffect, useRef, useState } from 'react'
import ConfirmModal from './components/ConfirmModal'
import ConnectionDrawer from './components/ConnectionDrawer'
import ErrorModal from './components/ErrorModal'
import TopBar, { type AppTab } from './components/TopBar'
import TopicsSidebar from './components/TopicsSidebar'
import ConsumePanel, { ConsumeToolbar } from './components/panels/ConsumePanel'
import TailPanel, { TailToolbar } from './components/panels/TailPanel'
import ProducePanel from './components/panels/ProducePanel'
import GroupsPanel from './components/panels/GroupsPanel'
import SchemasPanel from './components/panels/SchemasPanel'
import LogsPanel, { useBackendDebugSync } from './components/panels/LogsPanel'
import SettingsPanel from './components/panels/SettingsPanel'
import { useAppLogs } from './hooks/useAppLogs'
import { useKafdeskApp } from './hooks/useKafdeskApp'
import { useLocale } from './hooks/useLocale'
import { useGlass } from './hooks/useGlass'
import { useTheme } from './hooks/useTheme'
import { detectUpdateOs } from './lib/updateOs'
import { installWindowStatePersistence, restoreWindowState } from './lib/windowState'
import type { MsgKey } from './i18n'

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

  return (
    <div className={`app ${a.sidebarOpen ? 'drawer-open' : ''}`}>
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
              if (a.groups.length === 0) void a.loadGroups()
              else void a.loadLagSummary()
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
            busy={a.loadingMessages}
            onLoadMessages={a.loadMessages}
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
            partitionFilter={a.partitionFilter}
            setPartitionFilter={a.setPartitionFilter}
            tailing={a.tailing}
            busy={a.busy}
            onStartTail={a.startTail}
            onStopTail={a.stopTail}
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
                onOpenConsumerGroup={(groupId) => {
                  a.selectGroup(groupId)
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
                tailMessages={a.tailMessages}
                selectedTopic={a.selectedTopic}
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
                lagSummary={a.lagSummary}
                lagAutoRefresh={a.lagAutoRefresh}
                setLagAutoRefresh={a.setLagAutoRefresh}
                onLoadLagSummary={a.loadLagSummary}
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
              />
            )}
          </div>
        )}
      </main>
    </div>
  )
}
