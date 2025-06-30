<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\ClientController;
use Inertia\Inertia;

// Página inicial: redireciona para /clientes se autenticado, senão mostra tela de boas-vindas
Route::get('/', function () {
    if (auth()->check()) {
        return redirect()->route('clientes.index');
    }
    return redirect()->route('login');
})->name('home');

// Após login, redirecionar para a tela de clientes
Route::get('/dashboard', function () {
    return redirect()->route('clientes.index');
})->middleware(['auth', 'verified'])->name('dashboard');

// Rotas protegidas para CRUD de Clientes
Route::middleware(['auth', 'verified'])->group(function () {
    Route::get('/clientes', [ClientController::class, 'index'])->name('clientes.index');
    Route::get('/clientes/create', [ClientController::class, 'create'])->name('clientes.create');
    Route::post('/clientes', [ClientController::class, 'store'])->name('clientes.store');
    Route::get('/clientes/{client}', [ClientController::class, 'show'])->name('clientes.show');
    Route::get('/clientes/{client}/edit', [ClientController::class, 'edit'])->name('clientes.edit');
    Route::put('/clientes/{client}', [ClientController::class, 'update'])->name('clientes.update');
    Route::delete('/clientes/{client}', [ClientController::class, 'destroy'])->name('clientes.destroy');
});

// Rotas adicionais
require __DIR__.'/settings.php';
require __DIR__.'/auth.php';
