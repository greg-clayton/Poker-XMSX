# Story Point Poker 🃏

Real-time agile story point estimation for distributed teams.

## Quick Start (Local)

```bash
npm install
npm start
# Open http://localhost:3000
```

## Deploy to Railway (Free, ~2 mins)

1. Push to GitHub:
   ```bash
   git init
   git add .
   git commit -m "Story Point Poker"
   git remote add origin https://github.com/YOUR_USERNAME/story-point-poker.git
   git push -u origin main
   ```

2. Go to [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
3. Select your repo — Railway auto-detects Node.js and deploys
4. Click **Settings → Networking → Generate Domain** to get your public URL
5. Share that URL with your team 🎉

## Deploy to Render (Free)

1. Push to GitHub (same as above)
2. Go to [render.com](https://render.com) → **New Web Service**
3. Connect your repo
4. Set: Build Command = `npm install`, Start Command = `npm start`
5. Deploy → share the URL

## How to Play

- **First person to join becomes Admin** (shown with 👑)
- Admin sets the ticket/story being estimated
- Players pick from **3, 5, 7, 13** points
- Cards auto-reveal when everyone has voted
- Admin can force-reveal early with "Reveal Cards"
- Admin starts next round with "Next Round"
- Supports up to 8 players simultaneously
