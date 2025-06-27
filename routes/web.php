<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\ClientController;
use Inertia\Inertia;

Route::get('/', function () {
    return Inertia::render('Welcome');
})->name('home');

Route::get('/dashboard', function () {
    return Inertia::render('Dashboard');
})->middleware(['auth', 'verified'])->name('dashboard');

// CRUD de Clientes
Route::middleware(['auth', 'verified'])->group(function () {
    Route::get('/clientes', [ClientController::class, 'index'])->name('clientes.index');
    Route::get('/clientes/create', [ClientController::class, 'create'])->name('clientes.create');
    Route::post('/clientes', [ClientController::class, 'store'])->name('clientes.store');
    Route::get('/clientes/{client}', [ClientController::class, 'show'])->name('clientes.show');

    Route::get('/clientes/{client}/edit', [ClientController::class, 'edit'])->name('clientes.edit');
    Route::put('/clientes/{client}', [ClientController::class, 'update'])->name('clientes.update');
    Route::delete('/clientes/{client}', [ClientController::class, 'destroy'])->name('clientes.destroy');
});

require __DIR__.'/settings.php';
require __DIR__.'/auth.php';
