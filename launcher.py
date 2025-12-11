import subprocess
import sys
import time
import os

# --- CONFIGURATION ---
# List the config files for all the bots you want to run
BOT_CONFIGS = [
    "configs/bot1.env",
    "configs/bot2.env",
    "configs/bot3.env",
    "configs/bot4.env",
    "configs/bot5.env",
    "configs/bot6.env",
    "configs/bot7.env",
    "configs/bot8.env",
    "configs/bot9.env",
    "configs/bot10.env",
]

PYTHON_EXEC = sys.executable  # Automatically finds the current python (venv or system)
SCRIPT_NAME = "bot.py"  # Your main bot script name


def main():
    processes = []

    print(f"🚀 Starting {len(BOT_CONFIGS)} bots...")

    try:
        # Start each bot as a separate subprocess
        for config in BOT_CONFIGS:
            if not os.path.exists(config):
                print(f"⚠️ Config file not found: {config}")
                continue

            print(f"   ▶️ Launching bot with {config}...")

            # This is equivalent to running "python bot.py bot1.env" in terminal
            p = subprocess.Popen([PYTHON_EXEC, SCRIPT_NAME, config])
            processes.append(p)

        print(f"✅ All bots started! Press Ctrl+C to stop them all.")

        # Keep the main script alive to monitor the bots
        while True:
            time.sleep(1)

            # Optional: Check if a bot crashed and restart it
            for i, p in enumerate(processes):
                if p.poll() is not None:  # If process is not None, it ended
                    print(f"⚠️ Bot with {BOT_CONFIGS[i]} stopped/crashed. Restarting...")
                    processes[i] = subprocess.Popen([PYTHON_EXEC, SCRIPT_NAME, BOT_CONFIGS[i]])

    except KeyboardInterrupt:
        print("\n🛑 Stopping all bots...")
        for p in processes:
            p.terminate()  # Sends SIGTERM
            # p.kill()     # Use this if they refuse to close
        print("👋 All bots stopped.")


if __name__ == "__main__":
    main()