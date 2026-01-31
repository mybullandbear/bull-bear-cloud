# MyBullAndBearFire - Cloud Deployment

This folder contains the backend code ready for deployment to a Virtual Machine (Google Cloud Compute Engine, AWS EC2, or DigitalOcean Droplet).

## Prerequisites
1.  **Python 3.9+** installed on the server.
2.  **Firebase Project** set up (already configured with `serviceAccountKey.json`).
3.  **Fyers API Access** (App ID and Secret Key).

## Deployment Steps (Linux/Ubuntu)

1.  **Upload Code**:
    clone this repo or scp the folder to your server.

2.  **Install Dependencies**:
    ```bash
    cd BullBearCloud
    pip3 install -r requirements.txt
    ```

3.  **Run with Gunicorn (Production)**:
    ```bash
    # Run in background with 1 worker (essential for our thread logic)
    gunicorn -w 1 -b 0.0.0.0:80 app:app --daemon
    ```

4.  **Access**:
    Open `http://<YOUR_SERVER_IP>` in your browser.

## Important Note on Persistence
This app uses local SQLite databases (`data/*.db`) and local `token.json` for Fyers session.
*   **Token**: You will need to login once via `/connect` on the cloud instance to generate a fresh `token.json`.
*   **Data**: Option Chain history will persist in the `data/` folder on the server.

## Security
*   `serviceAccountKey.json` contains sensitive credentials. **Do not commit this to a public GitHub repository.**
*   If pushing to GitHub, add `serviceAccountKey.json` to `.gitignore` and upload it manually to the server.
