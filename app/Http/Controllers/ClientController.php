<?php

namespace App\Http\Controllers;

use App\Models\Client;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Inertia\Inertia;

class ClientController extends Controller
{
    /**
     * Display a listing of clients.
     */
    public function index()
    {
        $clients = Client::all();
        return Inertia::render('Clients/Index', compact('clients'));
    }

    /**
     * Show the form for creating a new client.
     */
    public function create()
    {
        return Inertia::render('Clients/Create');
    }

    /**
     * Store a newly created client in storage and create its database.
     */
    public function store(Request $request)
    {
        $data = $request->validate([
            'name'  => 'required|string|max:255',
            'email' => 'nullable|email|max:255',
        ]);

        try {
            Client::create($data);
            return redirect()->route('clientes.index')
                ->with('success', 'Cliente criado com sucesso.');
        } catch (\Exception $e) {
            Log::error('Error creating client: ' . $e->getMessage());
            return back()->withErrors(['error' => 'Erro ao criar cliente: ' . $e->getMessage()]);
        }
    }

    // app/Http/Controllers/ClientController.php

    public function show(Client $client)
    {
        try {
            $client->connectToDatabase();

            $driver = config('database.default');
            $tables = [];

            if ($driver === 'mysql') {
                $result = DB::connection('tenant')->select("SHOW TABLES");
                $tables = array_map(fn($row) => current((array) $row), $result);
            }

            if ($driver === 'sqlite') {
                $result = DB::connection('tenant')->select("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
                $tables = array_map(fn($row) => $row->name, $result);
            }

            return inertia('Clients/Show', [
                'client' => $client,
                'tables' => $tables,
            ]);
        } catch (\Exception $e) {
            return back()->withErrors([
                'error' => 'Erro ao conectar ao banco do cliente: ' . $e->getMessage(),
            ]);
        }
    }



    /**
     * Show the form for editing the specified client.
     */
    public function edit(Client $client)
    {
        return Inertia::render('Clients/Edit', compact('client'));
    }

    /**
     * Update the specified client in storage.
     */
    public function update(Request $request, Client $client)
    {
        $data = $request->validate([
            'name'  => 'required|string|max:255',
            'email' => 'nullable|email|max:255',
        ]);

        try {
            $client->update($data);
            return redirect()->route('clientes.index')
                ->with('success', 'Cliente atualizado com sucesso.');
        } catch (\Exception $e) {
            Log::error('Error updating client: ' . $e->getMessage());
            return back()->withErrors(['error' => 'Erro ao atualizar cliente: ' . $e->getMessage()]);
        }
    }

    /**
     * Remove the specified client and drop its database.
     */
    public function destroy(Client $client)
    {
        try {
            DB::statement("DROP DATABASE IF EXISTS `{$client->database_name}`");
            $client->delete();
            return redirect()->route('clientes.index')
                ->with('success', 'Cliente e banco removidos com sucesso.');
        } catch (\Exception $e) {
            Log::error('Error deleting client: ' . $e->getMessage());
            return back()->withErrors(['error' => 'Erro ao deletar cliente: ' . $e->getMessage()]);
        }
    }
}
