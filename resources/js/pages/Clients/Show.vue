<script setup lang="ts">
import { Head, Link } from '@inertiajs/vue3';
import AppLayout from '@/layouts/AppLayout.vue';
import { type BreadcrumbItem } from '@/types';
import { Button } from '@/components/ui/button';

const props = defineProps<{
    client: {
        id: string;
        name: string;
        email: string;
        slug: string;
        database_name: string;
    };
    tables: string[];
}>();

const breadcrumbs: BreadcrumbItem[] = [
    { title: 'Clientes', href: '/clientes' },
    { title: props.client.name, href: `/clientes/${props.client.id}` },
];

</script>

<template>
    <Head :title="props.client.name" />
    <AppLayout :breadcrumbs="breadcrumbs">
        <div class="p-6 max-w-3xl text-white">
            <h1 class="text-2xl font-semibold mb-4">Detalhes do Cliente</h1>
            <div class="mb-4">
                <span class="block text-sm text-gray-400">UUID</span>
                <p class="text-lg">{{ props.client.id }}</p>
            </div>

            <div class="space-y-4 flex gap-8 flex-wrap">
                <div>
                    <span class="block text-sm text-gray-400">Nome</span>
                    <p class="text-lg">{{ props.client.name }}</p>
                </div>
                <div>
                    <span class="block text-sm text-gray-400">Email</span>
                    <p class="text-lg">{{ props.client.email || '—' }}</p>
                </div>
                <div>
                    <span class="block text-sm text-gray-400">Slug</span>
                    <p class="text-lg">{{ props.client.slug }}</p>
                </div>
                <div>
                    <span class="block text-sm text-gray-400">Database</span>
                    <p class="text-lg">{{ props.client.database_name }}</p>
                </div>
            </div>

            <div>
                <span class="block text-sm text-gray-400">Tabelas no banco</span>
                <ul v-if="props.tables.length > 0" class="list-disc ml-4 mt-1 text-lg space-y-1">
                    <li v-for="table in props.tables" :key="table">{{ table }}</li>
                </ul>
                <p v-else class="text-gray-400 text-sm mt-1">Nenhuma tabela encontrada.</p>
            </div>

            <div class="mt-6 flex gap-2">
                <Link href="/clientes">
                    <Button variant="outline" class="text-red-600 border-red-600 hover:bg-red-900">Voltar</Button>
                </Link>
                <Link :href="`/clientes/${props.client.id}/edit`">
                    <Button class="text-white bg-[#292929] hover:bg-[#121212]">Editar</Button>
                </Link>
            </div>
        </div>
    </AppLayout>
</template>
