// resources/js/Pages/Clients/Index.vue
<script setup lang="ts">
import { Head, Link, useForm } from '@inertiajs/vue3';
import AppLayout from '@/layouts/AppLayout.vue';
import { type BreadcrumbItem } from '@/types';
import { Button } from '@/components/ui/button';
import { Plus, Edit2, Trash2 } from 'lucide-vue-next';

// Props conforme enviados pelo controller
const props = defineProps<{ clients: Array<{ id: string; name: string; email: string; slug: string; database_name: string }> }>();
const breadcrumbs: BreadcrumbItem[] = [
    { title: 'Clientes', href: '/clientes' },
];

const form = useForm();
function destroy(id: string) {
    if (!confirm('Tem certeza que deseja excluir este cliente?')) return;
    form.delete(`/clientes/${id}`);
}
</script>

<template>
    <Head title="Clientes" />
    <AppLayout :breadcrumbs="breadcrumbs">
        <div class="p-6">
            <div class="flex justify-between items-center mb-4">
                <h1 class="text-2xl font-semibold text-white">Clientes</h1>
                <Link href="/clientes/create">
                    <Button class="text-white bg-[#292929] hover:bg-[#121212]">
                        <Plus class="w-4 h-4 mr-2" />
                        Novo Cliente
                    </Button>
                </Link>
            </div>

            <div class="overflow-x-auto rounded-lg bg-[#121212] shadow">
                <table class="w-full text-left border-collapse">
                    <thead class="bg-[#121212]">
                    <tr>
                        <th class="px-4 py-2 text-white">Nome</th>
                        <th class="px-4 py-2 text-white">Email</th>
                        <th class="px-4 py-2 text-white">Slug</th>
                        <th class="px-4 py-2 text-white">DB</th>
                        <th class="px-4 py-2"></th>
                    </tr>
                    </thead>
                    <tbody>
                    <tr v-for="client in props.clients" :key="client.id" class="odd:bg-[#292929] even:bg-[#121212]">
                        <td class="px-4 py-3">
                            <Link :href="`/clientes/${client.id}`" class="text-white hover:underline">
                                {{ client.name }}
                            </Link>
                        </td>
                        <td class="px-4 py-3 text-white">{{ client.email || '—' }}</td>
                        <td class="px-4 py-3 text-white">{{ client.slug }}</td>
                        <td class="px-4 py-3 text-white">{{ client.database_name }}</td>
                        <td class="px-4 py-3 flex gap-2 justify-end">
                            <Link :href="`/clientes/${client.id}/edit`">
                                <Button variant="outline" size="sm" class="text-white border-white hover:bg-[#292929]">
                                    <Edit2 class="w-4 h-4" />
                                </Button>
                            </Link>
                            <Button variant="outline" size="sm" class="text-red-600 border-red-600 hover:bg-red-900" @click="destroy(client.id)">
                                <Trash2 class="w-4 h-4" />
                            </Button>
                        </td>
                    </tr>
                    <tr v-if="props.clients.length === 0">
                        <td colspan="5" class="py-4 text-center text-white">Nenhum cliente encontrado.</td>
                    </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </AppLayout>
</template>
