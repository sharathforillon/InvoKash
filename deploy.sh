#!/bin/bash

echo "🚀 InvoKash Production Deployment Script"
echo "========================================"

# Stop any running bot
echo "📍 Stopping existing bot..."
pkill -f "node bot.js"
sleep 2

# Check environment variables
if [ ! -f .env ]; then
    echo "❌ ERROR: .env file not found!"
    echo "Please create .env file with your API keys"
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Set secure permissions
echo "🔒 Setting secure permissions..."
chmod 600 .env
chmod 700 data invoices backups 2>/dev/null || true
chmod 600 data/*.json 2>/dev/null || true

# Create backup
echo "💾 Creating backup..."
mkdir -p backups
if [ -f data/profiles.json ]; then
    cp data/profiles.json backups/profiles_pre_deploy_$(date +%Y%m%d_%H%M%S).json
fi
if [ -f data/history.json ]; then
    cp data/history.json backups/history_pre_deploy_$(date +%Y%m%d_%H%M%S).json
fi

# Start bot
echo "🚀 Starting bot..."
NODE_ENV=production nohup node bot.js > bot.log 2>&1 &

sleep 3

# Check if bot is running
if ps aux | grep "node bot.js" | grep -v grep > /dev/null; then
    echo "✅ Bot started successfully!"
    echo ""
    echo "📊 Bot Status:"
    ps aux | grep "node bot.js" | grep -v grep
    echo ""
    echo "📝 View logs: tail -f ~/invoice-bot/bot.log"
    echo "🛑 Stop bot: pkill -f 'node bot.js'"
else
    echo "❌ ERROR: Bot failed to start"
    echo "Check logs: tail -50 ~/invoice-bot/bot.log"
    exit 1
fi
