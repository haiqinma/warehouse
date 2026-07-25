<script setup lang="ts">
import { computed, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { ElMessageBox } from 'element-plus'
import { Delete, DocumentCopy, Edit, Refresh } from '@element-plus/icons-vue'
import { useGroupStore } from '@/stores/groupStore'
import { copyText } from '@/utils/clipboard'
import { shortenAddress } from '@/utils/address'
import type { ManagedGroup, GroupMember } from '@/api'

const props = withDefaults(defineProps<{
  embedded?: boolean
}>(), {
  embedded: false
})

const groupStore = useGroupStore()
const emit = defineEmits<{
  (event: 'refresh'): void
}>()
const {
  managedGroups,
  groupMemberCounts,
  selectedGroupId,
  groupSearch,
  groupLoading,
  groupForm,
  groupSaving,
  memberForm,
  memberSaving,
  memberDialogVisible,
  filteredGroupMembers
} = storeToRefs(groupStore)
const {
  selectGroup,
  createGroup,
  renameGroup,
  removeGroup,
  openCreateMemberDialog,
  submitMember,
  resetMemberForm,
  editMember,
  removeMember
} = groupStore

const groupDialogVisible = ref(false)

const selectedGroup = computed(() =>
  managedGroups.value.find(group => group.id === selectedGroupId.value) || null
)
const inGroupDetail = computed(() => Boolean(selectedGroup.value))
const groupListKeyword = computed(() => groupSearch.value.trim().toLowerCase())
const visibleGroups = computed(() => {
  const keyword = groupListKeyword.value
  if (!keyword) return managedGroups.value
  return managedGroups.value.filter(group => group.name.toLowerCase().includes(keyword))
})
const detailMembers = computed(() => filteredGroupMembers.value)
const canManageSelectedGroup = computed(() => selectedGroup.value?.canManage === true)
const canInviteSelectedGroup = computed(() => selectedGroup.value?.canInvite === true)
const memberDialogTitle = computed(() => memberForm.value.id ? '编辑成员' : '添加成员')
const editingMember = computed(() => Boolean(memberForm.value.id))
const memberDialogGroupName = computed(() => {
  const groupID = memberForm.value.groupId || selectedGroup.value?.id || ''
  return managedGroups.value.find(group => group.id === groupID)?.name || selectedGroup.value?.name || '-'
})
const editingOwnName = computed(() => memberForm.value.editMode === 'name')
const editingOtherAlias = computed(() => memberForm.value.editMode === 'alias')
const creatingMember = computed(() => memberForm.value.editMode === 'create')

function showError(message: string, title = '错误') {
  void ElMessageBox.alert(message, title, {
    confirmButtonText: '确定',
    type: 'error',
    closeOnClickModal: false
  })
}

function copyMemberAddress(member: GroupMember) {
  const address = member.walletAddress?.trim()
  if (!address) {
    showError('暂无钱包地址')
    return
  }
  copyText(address, '钱包地址已复制')
}

function memberNameDisplay(member: GroupMember) {
  const name = member.name?.trim()
  const walletAddress = member.walletAddress?.trim().toLowerCase()
  if (name && name.toLowerCase() !== walletAddress) return name
  return '-'
}

function memberAliasDisplay(member: GroupMember) {
  return member.alias?.trim() || '-'
}

function openCreateGroupDialog() {
  groupForm.value.name = ''
  groupDialogVisible.value = true
}

function closeCreateGroupDialog() {
  groupDialogVisible.value = false
  groupForm.value.name = ''
}

async function submitCreateGroup() {
  const created = await createGroup()
  if (created) {
    groupDialogVisible.value = false
  }
}

function openGroupDetail(group: ManagedGroup) {
  selectGroup(group.id)
}

function backToGroupList() {
  selectGroup('all')
}

function openAddMemberForCurrentGroup() {
  if (!selectedGroup.value || !canInviteSelectedGroup.value) return
  openCreateMemberDialog()
  memberForm.value.groupId = selectedGroup.value.id
}

function groupActiveCount(group: ManagedGroup) {
  return groupMemberCounts.value.groups[group.id] || 0
}

function groupPendingCount(group: ManagedGroup) {
  return groupMemberCounts.value.pendingGroups[group.id] || 0
}

function groupRoleLabel(group: ManagedGroup) {
  return group.canManage ? '创建者' : '成员'
}

function groupRoleType(group: ManagedGroup) {
  return group.canManage ? 'success' : 'info'
}

function memberStatusLabel(member: GroupMember) {
  return member.status === 'pending' ? '待成员确认' : '已加入'
}

function memberStatusType(member: GroupMember) {
  return member.status === 'pending' ? 'warning' : 'success'
}

function memberRoleLabel(member: GroupMember) {
  return member.isOwner ? '创建者' : '成员'
}

function memberRoleType(member: GroupMember) {
  return member.isOwner ? 'success' : 'info'
}

function canManageMember(member: GroupMember) {
  return Boolean(member.canManage || canManageSelectedGroup.value)
}

</script>

<template>
  <div class="group-management-page" :class="{ embedded: props.embedded }">
    <div class="group-management-hero">
      <div class="group-management-title-row">
        <div v-if="!inGroupDetail" class="group-management-hero-main">
          <div class="group-management-title">分组管理</div>
          <div v-if="!inGroupDetail" class="group-management-sub">维护共享分组、成员准入和邀请确认，用于安全地控制协作范围。</div>
        </div>
        <div v-if="inGroupDetail" class="group-management-hero-main">
          <el-breadcrumb separator="/" class="group-breadcrumb">
            <el-breadcrumb-item>
              <button type="button" class="group-breadcrumb-link" @click="backToGroupList">分组管理</button>
            </el-breadcrumb-item>
            <el-breadcrumb-item>{{ selectedGroup?.name }}</el-breadcrumb-item>
          </el-breadcrumb>
        </div>
        <div v-else class="group-management-hero-actions">
          <el-button type="primary" @click="openCreateGroupDialog">新建分组</el-button>
          <el-tooltip content="刷新" placement="top">
            <el-button
              class="refresh-button"
              circle
              :icon="Refresh"
              :disabled="groupLoading"
              :class="{ 'is-refreshing': groupLoading }"
              @click="emit('refresh')"
            />
          </el-tooltip>
        </div>
      </div>
    </div>

    <div v-if="!inGroupDetail" class="group-management-list-page">
      <div class="group-management-toolbar">
        <div class="toolbar-left">
          <el-input v-model="groupSearch" clearable placeholder="搜索分组" />
        </div>
      </div>

      <div v-if="!visibleGroups.length" class="group-management-empty">暂无分组</div>
      <el-table
        v-else
        :data="visibleGroups"
        size="small"
        class="group-list-table"
        row-class-name="group-list-table-row"
        @row-click="openGroupDetail"
      >
        <el-table-column prop="name" label="分组名称" min-width="180">
          <template #default="{ row }">
            <span class="group-list-name">{{ row.name }}</span>
          </template>
        </el-table-column>
        <el-table-column label="身份" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="groupRoleType(row)" effect="plain">{{ groupRoleLabel(row) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="成员" width="100">
          <template #default="{ row }">{{ groupActiveCount(row) }}</template>
        </el-table-column>
        <el-table-column label="待确认" width="100">
          <template #default="{ row }">
            <el-tag v-if="groupPendingCount(row)" size="small" type="warning" effect="plain">
              {{ groupPendingCount(row) }}
            </el-tag>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="140">
          <template #default="{ row }">
            <div v-if="row.canManage" class="group-list-actions" @click.stop>
              <el-button size="small" text @click="renameGroup(row)">重命名</el-button>
              <el-button size="small" text type="danger" @click="removeGroup(row)">删除</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <div v-else class="group-management-detail-page">
      <div class="group-management-toolbar">
        <div class="toolbar-left">
          <el-input v-model="groupSearch" clearable placeholder="搜索名称 / 别名 / 钱包" />
        </div>
        <div class="toolbar-right">
          <el-button
            v-if="canInviteSelectedGroup"
            type="primary"
            @click="openAddMemberForCurrentGroup"
          >
            添加成员
          </el-button>
          <el-tooltip content="刷新" placement="top">
            <el-button
              class="refresh-button"
              circle
              :icon="Refresh"
              :disabled="groupLoading"
              :class="{ 'is-refreshing': groupLoading }"
              @click="emit('refresh')"
            />
          </el-tooltip>
        </div>
      </div>

      <el-empty v-if="!detailMembers.length" description="暂无成员" />
      <el-table v-else :data="detailMembers" size="small" class="member-list-table">
        <el-table-column label="名称" min-width="150">
          <template #default="{ row }">
            <span class="member-name-text">{{ memberNameDisplay(row) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="别名" min-width="150">
          <template #default="{ row }">
            <span class="member-alias-text">{{ memberAliasDisplay(row) }}</span>
          </template>
        </el-table-column>
        <el-table-column label="身份" width="100">
          <template #default="{ row }">
            <el-tag size="small" :type="memberRoleType(row)" effect="plain">{{ memberRoleLabel(row) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="钱包地址" min-width="210">
          <template #default="{ row }">
            <div class="member-address-cell">
              <span class="mono wallet-text" :title="row.walletAddress">{{ shortenAddress(row.walletAddress) }}</span>
              <el-tooltip content="复制钱包地址" placement="top">
                <el-button
                  class="icon-button icon-button-inline"
                  link
                  :icon="DocumentCopy"
                  :disabled="!row.walletAddress"
                  @click="copyMemberAddress(row)"
                />
              </el-tooltip>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag size="small" :type="memberStatusType(row)" effect="plain">
              {{ memberStatusLabel(row) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="150">
          <template #default="{ row }">
            <div class="member-actions">
              <el-button size="small" text :icon="Edit" @click="editMember(row)">编辑</el-button>
              <el-tooltip v-if="canManageMember(row)" content="移除" placement="top">
                <el-button class="icon-button" type="danger" link :icon="Delete" @click="removeMember(row)" />
              </el-tooltip>
            </div>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <el-dialog
      v-model="groupDialogVisible"
      title="新建分组"
      width="420px"
      @closed="closeCreateGroupDialog"
    >
      <div class="group-dialog-body">
        <el-input
          v-model="groupForm.name"
          placeholder="分组名称"
          @keyup.enter="submitCreateGroup"
        />
      </div>
      <template #footer>
        <el-button @click="closeCreateGroupDialog">取消</el-button>
        <el-button type="primary" :loading="groupSaving" @click="submitCreateGroup">创建</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="memberDialogVisible"
      :title="memberDialogTitle"
      width="520px"
      class="member-dialog"
      @closed="resetMemberForm"
    >
      <div class="member-dialog-body">
        <el-form label-position="top" class="member-dialog-form">
          <el-form-item label="当前分组">
            <el-input
              :model-value="memberDialogGroupName"
              readonly
            />
          </el-form-item>
          <el-form-item v-if="editingMember" label="钱包地址">
            <el-input
              v-model="memberForm.walletAddress"
              readonly
            />
          </el-form-item>
          <el-form-item v-else label="用户名或钱包地址">
            <el-input
              v-model="memberForm.target"
              clearable
              placeholder="输入用户名或钱包地址"
              @keyup.enter="submitMember"
            />
          </el-form-item>
          <el-form-item v-if="editingOwnName" label="名称">
            <el-input
              v-model="memberForm.name"
              placeholder="可选"
              @keyup.enter="submitMember"
            />
          </el-form-item>
          <el-form-item v-if="editingOtherAlias || creatingMember" label="别名">
            <el-input
              v-model="memberForm.alias"
              placeholder="可选，仅自己可见"
              @keyup.enter="submitMember"
            />
          </el-form-item>
        </el-form>
      </div>
      <template #footer>
        <el-button @click="memberDialogVisible = false">取消</el-button>
        <el-button
          type="primary"
          :loading="memberSaving"
          @click="submitMember"
        >
          {{ editingMember ? '保存' : '添加' }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style lang="scss" scoped>
.group-management-page {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 8px;
  min-height: 0;
}

.group-management-page.embedded {
  padding: 0;
  width: 100%;
  flex: 1;
}

.group-management-page.embedded .group-management-title {
  font-size: 14px;
  line-height: 1.4;
}

.group-management-page.embedded .group-management-sub {
  margin-top: 4px;
  font-size: 12px;
  line-height: 1.5;
}

.card-title {
  font-size: 14px;
  font-weight: 600;
  color: #1f2d3d;
}

.card-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: #909399;
}

.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  font-size: 12px;
}

.group-management-hero,
.group-management-list-page,
.group-management-detail-page {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 0;
}

.group-management-title-row,
.group-management-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.group-management-hero-main {
  min-width: 0;
}

.group-management-hero-actions,
.toolbar-left,
.toolbar-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.toolbar-left {
  flex: 1;
}

.group-management-title {
  font-size: 20px;
  font-weight: 600;
  color: #1f2d3d;
}

.group-management-sub {
  font-size: 13px;
  color: #909399;
}

.group-breadcrumb {
  min-width: 0;
}

.group-breadcrumb-link {
  border: 0;
  background: transparent;
  padding: 0;
  color: #409eff;
  font: inherit;
  cursor: pointer;
}

.group-breadcrumb-link:hover {
  color: #337ecc;
}

.group-list-table :deep(.group-list-table-row) {
  cursor: pointer;
}

.group-list-table :deep(.group-list-table-row:hover > td.el-table__cell) {
  background: #f3f8ff;
}

.group-list-name {
  font-size: 14px;
  font-weight: 400;
  color: #1f2d3d;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.group-list-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.member-form {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.member-form :deep(.el-select) {
  width: 100%;
}

.group-dialog-body {
  padding-top: 4px;
}

.member-dialog-body {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.member-dialog-form {
  display: flex;
  flex-direction: column;
}

.member-address-cell,
.member-actions {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  flex-wrap: wrap;
}

.member-actions {
  justify-content: flex-start;
  flex-shrink: 0;
  padding-top: 2px;
}

.wallet-text {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.member-name-text,
.member-alias-text {
  min-width: 0;
  line-height: 1.4;
  white-space: normal;
  overflow-wrap: anywhere;
  word-break: break-word;
}

.icon-button {
  padding: 0 4px;
}

.icon-button-inline {
  padding: 0;
}

.group-management-empty {
  font-size: 12px;
  color: #909399;
}

.group-management-empty {
  padding: 8px;
}

@media (max-width: 900px) {
  .group-management-title-row,
  .group-management-toolbar {
    flex-direction: column;
    align-items: stretch;
  }

  .toolbar-right,
  .group-management-hero-actions {
    justify-content: flex-start;
  }

  .member-form {
    grid-template-columns: 1fr;
  }
}
</style>
