<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use App\Models\User;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Str;

class CreateTestUser extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'app:create-test-user';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Create a test user for API testing';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        // Check if user already exists
        $existingUser = User::where('email', 'admin@example.com')->first();
        
        if ($existingUser) {
            $this->info('User admin@example.com already exists.');
            $this->info('ID: ' . $existingUser->id);
            $this->info('Identifier: ' . $existingUser->identifier);
            return;
        }

        // Create new user
        $user = User::create([
            'name' => 'Admin User',
            'email' => 'admin@example.com',
            'password' => Hash::make('password'),
            'identifier' => Str::uuid(),
        ]);

        $this->info('Test user created successfully!');
        $this->info('Email: admin@example.com');
        $this->info('Password: password');
        $this->info('Identifier: ' . $user->identifier);
    }
}
