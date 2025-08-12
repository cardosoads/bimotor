<template>
    <AppLayout title="Clonagem de Dados">
        <template #header>
            <h2 class="font-semibold text-xl text-gray-800 leading-tight">
                Clonagem de Dados entre Clientes
            </h2>
        </template>

        <div class="py-12">
            <div class="max-w-7xl mx-auto sm:px-6 lg:px-8">
                <div class="bg-white overflow-hidden shadow-xl sm:rounded-lg">
                    <div class="p-6 lg:p-8 bg-white border-b border-gray-200">
                        <h1 class="text-2xl font-medium text-gray-900">
                            Clonagem de Banco de Dados
                        </h1>
                        <p class="mt-6 text-gray-500 leading-relaxed">
                            Clone tabelas e dados entre diferentes clientes de forma segura e eficiente.
                        </p>
                    </div>

                    <div class="bg-gray-200 bg-opacity-25 grid grid-cols-1 md:grid-cols-2 gap-6 lg:gap-8 p-6 lg:p-8">
                        <!-- Formulário de Clonagem -->
                        <div class="bg-white p-6 rounded-lg shadow">
                            <h3 class="text-lg font-semibold text-gray-900 mb-4">
                                Configuração da Clonagem
                            </h3>
                            
                            <form @submit.prevent="executeClone" class="space-y-4">
                                <!-- Cliente de Origem -->
                                <div>
                                    <label for="source_client" class="block text-sm font-medium text-gray-700">
                                        Cliente de Origem
                                    </label>
                                    <input
                                        id="source_client"
                                        v-model="form.source_client"
                                        type="text"
                                        class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                                        placeholder="ID ou nome do cliente de origem"
                                        required
                                    />
                                </div>

                                <!-- Cliente de Destino -->
                                <div>
                                    <label for="target_client" class="block text-sm font-medium text-gray-700">
                                        Cliente de Destino
                                    </label>
                                    <input
                                        id="target_client"
                                        v-model="form.target_client"
                                        type="text"
                                        class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                                        placeholder="ID ou nome do cliente de destino"
                                        required
                                    />
                                </div>

                                <!-- Tabelas -->
                                <div>
                                    <label for="tables" class="block text-sm font-medium text-gray-700">
                                        Tabelas (uma por linha)
                                    </label>
                                    <textarea
                                        id="tables"
                                        v-model="tablesText"
                                        rows="4"
                                        class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500"
                                        placeholder="users&#10;products&#10;orders"
                                        required
                                    ></textarea>
                                </div>

                                <!-- Opções -->
                                <div class="space-y-2">
                                    <div class="flex items-center">
                                        <input
                                            id="include_structure"
                                            v-model="form.include_structure"
                                            type="checkbox"
                                            class="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
                                        />
                                        <label for="include_structure" class="ml-2 block text-sm text-gray-900">
                                            Incluir estrutura das tabelas
                                        </label>
                                    </div>

                                    <div class="flex items-center">
                                        <input
                                            id="include_data"
                                            v-model="form.include_data"
                                            type="checkbox"
                                            class="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
                                        />
                                        <label for="include_data" class="ml-2 block text-sm text-gray-900">
                                            Incluir dados das tabelas
                                        </label>
                                    </div>

                                    <div class="flex items-center">
                                        <input
                                            id="overwrite"
                                            v-model="form.overwrite"
                                            type="checkbox"
                                            class="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
                                        />
                                        <label for="overwrite" class="ml-2 block text-sm text-gray-900">
                                            Sobrescrever tabelas existentes
                                        </label>
                                    </div>
                                </div>

                                <!-- Botão de Execução -->
                                <div class="pt-4">
                                    <button
                                        type="submit"
                                        :disabled="isLoading"
                                        class="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed"
                                    >
                                        <svg v-if="isLoading" class="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                                            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                                            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                        </svg>
                                        {{ isLoading ? 'Clonando...' : 'Executar Clonagem' }}
                                    </button>
                                </div>
                            </form>
                        </div>

                        <!-- Resultados -->
                        <div class="bg-white p-6 rounded-lg shadow">
                            <h3 class="text-lg font-semibold text-gray-900 mb-4">
                                Resultados da Clonagem
                            </h3>
                            
                            <div v-if="!result && !error" class="text-gray-500 text-center py-8">
                                Execute uma clonagem para ver os resultados aqui.
                            </div>

                            <!-- Sucesso -->
                            <div v-if="result" class="space-y-4">
                                <div class="bg-green-50 border border-green-200 rounded-md p-4">
                                    <div class="flex">
                                        <div class="flex-shrink-0">
                                            <svg class="h-5 w-5 text-green-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                                                <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd" />
                                            </svg>
                                        </div>
                                        <div class="ml-3">
                                            <h3 class="text-sm font-medium text-green-800">
                                                {{ result.message }}
                                            </h3>
                                            <div class="mt-2 text-sm text-green-700">
                                                <p>{{ result.summary }}</p>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <!-- Estatísticas -->
                                <div v-if="result.statistics" class="bg-blue-50 border border-blue-200 rounded-md p-4">
                                    <h4 class="text-sm font-medium text-blue-800 mb-2">Estatísticas</h4>
                                    <div class="grid grid-cols-2 gap-2 text-sm text-blue-700">
                                        <div>Tabelas clonadas: {{ result.statistics.cloned_tables }}</div>
                                        <div>Total de linhas: {{ result.statistics.total_rows }}</div>
                                        <div>Tempo de processamento: {{ result.statistics.processing_time }}s</div>
                                        <div>Erros: {{ result.statistics.errors }}</div>
                                    </div>
                                </div>

                                <!-- Detalhes -->
                                <div v-if="result.details" class="space-y-2">
                                    <h4 class="text-sm font-medium text-gray-900">Detalhes:</h4>
                                    <div class="bg-gray-50 rounded-md p-3 max-h-40 overflow-y-auto">
                                        <div v-for="detail in result.details" :key="detail" class="text-sm text-gray-700">
                                            {{ detail }}
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <!-- Erro -->
                            <div v-if="error" class="bg-red-50 border border-red-200 rounded-md p-4">
                                <div class="flex">
                                    <div class="flex-shrink-0">
                                        <svg class="h-5 w-5 text-red-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                                            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
                                        </svg>
                                    </div>
                                    <div class="ml-3">
                                        <h3 class="text-sm font-medium text-red-800">
                                            Erro na Clonagem
                                        </h3>
                                        <div class="mt-2 text-sm text-red-700">
                                            <p>{{ error }}</p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </AppLayout>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { router } from '@inertiajs/vue3'
import AppLayout from '@/Layouts/AppLayout.vue'

// Estado reativo
const isLoading = ref(false)
const result = ref(null)
const error = ref(null)
const tablesText = ref('')

// Formulário
const form = ref({
    source_client: '',
    target_client: '',
    tables: [],
    include_structure: true,
    include_data: true,
    overwrite: false
})

// Computed para converter texto das tabelas em array
const tables = computed(() => {
    return tablesText.value
        .split('\n')
        .map(table => table.trim())
        .filter(table => table.length > 0)
})

// Função para executar a clonagem
const executeClone = async () => {
    if (tables.value.length === 0) {
        error.value = 'Por favor, informe pelo menos uma tabela para clonar.'
        return
    }

    isLoading.value = true
    result.value = null
    error.value = null

    try {
        const response = await fetch('/api/clone', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${window.Laravel.auth_token}`,
                'X-CSRF-TOKEN': document.querySelector('meta[name="csrf-token"]')?.getAttribute('content') || ''
            },
            body: JSON.stringify({
                ...form.value,
                tables: tables.value
            })
        })

        const data = await response.json()

        if (data.success) {
            result.value = data
        } else {
            error.value = data.message || 'Erro desconhecido na clonagem'
        }
    } catch (err) {
        error.value = 'Erro de conexão: ' + err.message
    } finally {
        isLoading.value = false
    }
}
</script>