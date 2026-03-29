#!/bin/bash
#
# PGit Hook Installation Script
#
# This script installs the pgit pre-commit hook into your Git repository.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR=".git/hooks"
PRE_COMMIT_HOOK="$GIT_HOOKS_DIR/pre-commit"

echo "🔧 PGit Hook Installer"
echo "====================="
echo ""

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "❌ Error: Not a git repository. Run 'git init' first."
    exit 1
fi

# Create hooks directory if it doesn't exist
mkdir -p "$GIT_HOOKS_DIR"

# Check if pre-commit hook already exists
if [ -f "$PRE_COMMIT_HOOK" ]; then
    echo "⚠️  Existing pre-commit hook found!"
    echo ""
    echo "Choose an option:"
    echo "  1) Backup existing hook and install pgit hook"
    echo "  2) Append pgit check to existing hook"
    echo "  3) Cancel"
    echo ""
    read -p "Enter choice (1-3): " choice
    
    case $choice in
        1)
            BACKUP_NAME="pre-commit.backup.$(date +%Y%m%d%H%M%S)"
            mv "$PRE_COMMIT_HOOK" "$GIT_HOOKS_DIR/$BACKUP_NAME"
            echo "✓ Backed up existing hook to $BACKUP_NAME"
            ;;
        2)
            echo "" >> "$PRE_COMMIT_HOOK"
            echo "# PGit check (appended)" >> "$PRE_COMMIT_HOOK"
            cat "$SCRIPT_DIR/pre-commit" >> "$PRE_COMMIT_HOOK"
            chmod +x "$PRE_COMMIT_HOOK"
            echo "✓ Appended pgit check to existing hook"
            exit 0
            ;;
        3)
            echo "Cancelled."
            exit 0
            ;;
        *)
            echo "Invalid choice."
            exit 1
            ;;
    esac
fi

# Install the hook
cp "$SCRIPT_DIR/pre-commit" "$PRE_COMMIT_HOOK"
chmod +x "$PRE_COMMIT_HOOK"

echo ""
echo "✅ Pre-commit hook installed successfully!"
echo ""
echo "Next steps:"
echo "  1. Create a .pgit-config file in your repository root:"
echo "     echo 'my_dataset=/path/to/data.csv' > .pgit-config"
echo ""
echo "  2. Commit your baseline data:"
echo "     pgit commit /path/to/data.csv my_dataset -m 'Initial baseline'"
echo ""
echo "  3. Your next git commit will automatically check for drift!"
echo ""
echo "To uninstall:"
echo "  rm .git/hooks/pre-commit"
