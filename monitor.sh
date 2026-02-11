#!/bin/bash

# Check if bot is running
if ! ps aux | grep "node bot.js" | grep -v grep > /dev/null; then
    echo "❌ Bot is not running! Restarting..."
    cd ~/invoice-bot
    ./deploy.sh
    echo "📧 Alert: Bot was down and has been restarted" | mail -s "InvoKash Alert" your@email.com
fi
