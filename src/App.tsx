import ConnectionDrawer from './components/ConnectionDrawer'
import TopBar from './components/TopBar'
import TopicsSidebar from './components/TopicsSidebar'
import ConsumePanel, { ConsumeToolbar } from './components/panels/ConsumePanel'
import TailPanel, { TailToolbar } from './components/panels/TailPanel'
import ProducePanel from './components/panels/ProducePanel'
import GroupsPanel from './components/panels/GroupsPanel'
import LogsPanel from './components/panels/LogsPanel'
import SettingsPanel from './components/panels/SettingsPanel'
import { useAppLogs } from './hooks/useAppLogs'
import { useKafdeskApp } from './hooks/useKafdeskApp'
import { useLocale } from './hooks/useLocale'
import { useTheme } from './hooks/useTheme'

export default function App() {
  const { theme, toggleTheme } = useTheme()
  const { tr, toggleLocale } = useLocale()
  const { logs, setLogs, pushLog } = useAppLogs()
  const a = useKafdeskApp(pushLog)

  if (!a.config) return null

  return (
    <div className={`app ${a.sidebarOpen ? 'drawer-open' : ''}`}>
      <ConnectionDrawer
        tr={tr}
        sidebarOpen={a.sidebarOpen}
        setSidebarOpen={a.setSidebarOpen}
        connections={a.connections}
        activeId={a.activeId}
        pendingDeleteId={a.pendingDeleteId}
        config={a.config}
        authMethod={a.authMethod}
        authSession={a.authSession}
        brokersText={a.brokersText}
        setBrokersText={a.setBrokersText}
        connected={a.connected}
        srStatus={a.srStatus}
        busy={a.busy}
        pingInfo={a.pingInfo}
        error={a.error}
        detailsRef={a.detailsRef}
        onAddConnection={a.addConnection}
        onSelectConnection={a.selectConnection}
        onEditConnection={a.editConnection}
        onDuplicateConnection={a.duplicateConnection}
        onRequestDeleteConnection={a.requestDeleteConnection}
        onCancelDeleteConnection={a.cancelDeleteConnection}
        onConfirmDeleteConnection={a.confirmDeleteConnection}
        onUpdateConfig={a.updateConfig}
        onSetAuthMethod={a.setAuthMethod}
        onSsoLogin={a.ssoLogin}
        onSsoLogout={a.ssoLogout}
        onSaveConnectionDetails={a.saveConnectionDetails}
        onConnect={a.connect}
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
          onOpenSettings={() => a.setTab('settings')}
          settingsActive={a.tab === 'settings'}
          config={a.config}
          connected={a.connected}
          srStatus={a.srStatus}
          authSession={a.authSession}
        />

        <div className="tabs">
          <button
            className={`tab ${a.tab === 'consume' ? 'active' : ''}`}
            onClick={() => a.setTab('consume')}
          >
            {tr('consume')}
          </button>
          <button
            className={`tab ${a.tab === 'produce' ? 'active' : ''}`}
            onClick={() => a.setTab('produce')}
          >
            {tr('produce')}
          </button>
          <button
            className={`tab ${a.tab === 'tail' ? 'active' : ''}`}
            onClick={() => a.setTab('tail')}
          >
            {tr('tail')}
          </button>
          <button
            className={`tab ${a.tab === 'groups' ? 'active' : ''}`}
            onClick={() => {
              a.setTab('groups')
              if (a.connected) {
                if (a.groups.length === 0) void a.loadGroups()
                else void a.loadLagSummary()
              }
            }}
          >
            {tr('groups')}
          </button>
          <button
            className={`tab ${a.tab === 'logs' ? 'active' : ''}`}
            onClick={() => a.setTab('logs')}
          >
            {tr('logs')}
          </button>
          {a.selectedTopic && a.tab !== 'logs' && a.tab !== 'settings' && (
            <span className="tab-topic">
              {tr('topic')}: <strong>{a.selectedTopic}</strong>
            </span>
          )}
        </div>

        {a.tab === 'consume' && (
          <ConsumeToolbar
            tr={tr}
            selectedTopic={a.selectedTopic}
            topicDetails={a.topicDetails}
            search={a.search}
            setSearch={a.setSearch}
            searchKey={a.searchKey}
            setSearchKey={a.setSearchKey}
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
            searchKey={a.searchKey}
            setSearchKey={a.setSearchKey}
            partitionFilter={a.partitionFilter}
            setPartitionFilter={a.setPartitionFilter}
            tailing={a.tailing}
            busy={a.busy}
            onStartTail={a.startTail}
            onStopTail={a.stopTail}
          />
        )}

        {a.tab === 'logs' ? (
          <LogsPanel tr={tr} logs={logs} onClear={() => setLogs([])} />
        ) : a.tab === 'settings' ? (
          <SettingsPanel
            tr={tr}
            theme={theme}
            toggleTheme={toggleTheme}
            toggleLocale={toggleLocale}
            updateStatus={a.updateStatus}
            updateAuto={a.updateAuto}
            onSetAutoUpdate={a.setAutoUpdateEnabled}
            onCheckUpdates={a.checkForUpdates}
          />
        ) : (
          <div
            className={`content ${a.tab === 'produce' ? 'content-produce' : ''} ${a.tab === 'groups' ? 'content-groups' : ''}`}
          >
            <TopicsSidebar
              tr={tr}
              connected={a.connected}
              filteredTopics={a.filteredTopics}
              topicQuery={a.topicQuery}
              setTopicQuery={a.setTopicQuery}
              selectedTopic={a.selectedTopic}
              onSelectTopic={a.selectTopic}
            />

            {a.tab === 'consume' && (
              <ConsumePanel
                tr={tr}
                topicDetails={a.topicDetails}
                messages={a.messages}
                selectedTopic={a.selectedTopic}
                busy={a.loadingMessages}
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
                produceKey={a.produceKey}
                setProduceKey={a.setProduceKey}
                produceValue={a.produceValue}
                setProduceValue={a.setProduceValue}
                busy={a.busy}
                onSendMessage={a.sendMessage}
                produceResult={a.produceResult}
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
          </div>
        )}
      </main>
    </div>
  )
}
