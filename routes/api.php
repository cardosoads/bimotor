<?php

use App\Http\Controllers\Api\LoginController;
use App\Http\Controllers\Api\ReceiveDataController;
use App\Models\User;
use App\Models\Client;
use Illuminate\Support\Facades\Auth;
use Illuminate\Support\Facades\Request;
use Illuminate\Support\Facades\Route;

Route::post('/login', LoginController::class);

// todas as rotas abaixo precisam de token Sanctum
Route::middleware('auth:sanctum')->group(function () {
    // endpoint para o app buscar o identifier
    Route::get('/me/identifier', function (\Illuminate\Http\Request $request) {
        return response()->json([
            'identifier' => $request->user()->identifier,
        ]);
    });
    
    // endpoint para listar clientes disponíveis
    Route::get('/clients', function (\Illuminate\Http\Request $request) {
        $clients = Client::select('id', 'name', 'database_name', 'email')
            ->orderBy('name')
            ->get();
            
        return response()->json([
            'data' => $clients,
            'total' => $clients->count()
        ]);
    });

    // recebimento de dados
    Route::post('/receive', [ReceiveDataController::class, 'store']);
    Route::get('/received-data', [ReceiveDataController::class, 'index']);

    Route::post('/verify-record', [ReceiveDataController::class, 'verifyRecord']);
    Route::get('/verify-table/{table}', [ReceiveDataController::class, 'verifyTable']);
});

Route::middleware('auth:sanctum')->group(function () {
    // Endpoint para integração com BI
    Route::post('/connectbi', [ReceiveDataController::class, 'connectBI']);
});
