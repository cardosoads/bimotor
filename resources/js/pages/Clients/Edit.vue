// resources/js/Pages/Clients/Edit.vue
<script setup lang="ts">
import { Head, Link, useForm } from '@inertiajs/vue3';
import AppLayout from '@/layouts/AppLayout.vue';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import InputError from '@/components/InputError.vue';
import { type BreadcrumbItem } from '@/types';

const props = defineProps<{ client: { id: string; name: string; email: string } }>();
const form = useForm({ name: props.client.name, email: props.client.email });
const breadcrumbs: BreadcrumbItem[] = [
    { title: 'Clientes', href: '/clientes' },
    { title: 'Editar', href: `/clientes/${props.client.id}/edit` },
];

function submit() {
    form.put(`/clientes/${props.client.id}`, { onFinish: () => {} });
}
</script>

<template>
    <Head title="Editar Cliente" />
    <AppLayout :breadcrumbs="breadcrumbs">
        <div class="p-6 max-w-lg">
            <h1 class="text-2xl font-semibold text-white mb-4">Editar Cliente</h1>
            <form @submit.prevent="submit" class="space-y-6">
                <div class="grid gap-2">
                    <Label for="name" class="text-white">Nome</Label>
                    <Input id="name" v-model="form.name" required class="text-white bg-[#0a0a0a] border-[#292929]" />
                    <InputError :message="form.errors.name" />
                </div>
                <div class="grid gap-2">
                    <Label for="email" class="text-white">Email</Label>
                    <Input id="email" type="email" v-model="form.email" class="text-white bg-[#0a0a0a] border-[#292929]" />
                    <InputError :message="form.errors.email" />
                </div>
                <div class="flex justify-end gap-2">
                    <Link href="/clientes">
                        <Button variant="outline" class="text-red-600 border-red-600 hover:bg-red-900">Cancelar</Button>
                    </Link>
                    <Button type="submit" :disabled="form.processing" class="text-white bg-[#292929] hover:bg-[#121212]">Atualizar</Button>
                </div>
            </form>
        </div>
    </AppLayout>
</template>
