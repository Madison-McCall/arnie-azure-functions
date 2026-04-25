## Local settings

This project uses a `local.settings.json` file for local Azure Functions configuration and secrets.

To run the project locally:

1. Copy `local.settings.example.json`.
2. Rename the copy to `local.settings.json`.
3. Replace the placeholder values inside `{}` with your own Azure Function app settings, storage connection string, and Azure SQL credentials.

Do not commit `local.settings.json` to GitHub. It may contain secrets and is intentionally ignored by Git.