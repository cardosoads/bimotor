<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Concerns\HasUuids;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Notifications\Notifiable;
use Illuminate\Support\Str;
use Illuminate\Support\Facades\DB;

class Client extends Model
{
    use HasFactory, Notifiable, HasUuids;

    // Desativa o auto-incremento e define o tipo de chave como string (UUID)
    public $incrementing = false;
    protected $keyType = 'string';

    // Campos que podem ser atribuídos em massa
    protected $fillable = [
        'name',
        'slug',
        'email',
        'database_name',
    ];

    // Boot para definir slug e nome do banco antes de criar
    protected static function booted()
    {
        static::creating(function (Client $client) {
            if (empty($client->slug)) {
                $client->slug = Str::slug($client->name);
            }

            if (empty($client->database_name)) {
                $client->database_name = 'client_' . Str::slug($client->name, '_');
            }
        });

        static::created(function (Client $client) {
            $driver = config('database.default');
            $dbName = $client->database_name;

            if ($driver === 'mysql') {
                DB::statement(sprintf(
                    'CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci',
                    $dbName
                ));
            }

            if ($driver === 'sqlite') {
                $path = database_path("tenants/{$dbName}.sqlite");

                if (!file_exists(dirname($path))) {
                    mkdir(dirname($path), 0755, true);
                }

                if (!file_exists($path)) {
                    file_put_contents($path, '');
                }
            }
        });
    }

    public function connectToDatabase(): void
    {
        $driver = config('database.default');
        $dbName = $this->database_name;

        if ($driver === 'mysql') {
            config([
                'database.connections.tenant' => array_merge(
                    config('database.connections.mysql'),
                    ['database' => $dbName]
                )
            ]);
        }

        if ($driver === 'sqlite') {
            config([
                'database.connections.tenant' => array_merge(
                    config('database.connections.sqlite'),
                    ['database' => database_path("tenants/{$dbName}.sqlite")]
                )
            ]);
        }

        DB::purge('tenant');
        DB::reconnect('tenant');
    }

}
