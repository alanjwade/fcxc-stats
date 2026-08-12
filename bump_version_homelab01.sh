#!/bin/bash

# Cross Country Statistics Tracker - Bump Version Tag
#
# Checks for uncommitted changes, bumps the version tag in homelab-deployment/docker-compose.yml,
# and commits the change to git with an automatic push.
#
# Usage:
#   ./bump_version_homelab01.sh
#   ./bump_version_homelab01.sh minor  # bump minor version instead of patch
#   ./bump_version_homelab01.sh major  # bump major version instead of patch

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="homelab-deployment/docker-compose.yml"
BUMP_TYPE="${1:-patch}"  # Default to patch version bump

# Validate bump type
if [[ ! "$BUMP_TYPE" =~ ^(major|minor|patch)$ ]]; then
    echo -e "${RED}Error: Invalid bump type '$BUMP_TYPE'. Must be 'major', 'minor', or 'patch'.${NC}"
    exit 1
fi

echo -e "${GREEN}Cross Country Statistics Tracker - Version Bump${NC}"
echo "=================================================="
echo "Bump type: ${BLUE}${BUMP_TYPE}${NC}"
echo ""

# Check if git is available and we're in a git repo
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo -e "${RED}Error: Not in a git repository.${NC}"
    exit 1
fi

# Check for uncommitted changes
echo -e "${YELLOW}Checking for uncommitted changes...${NC}"
if ! git diff-index --quiet HEAD --; then
    echo -e "${RED}Error: Uncommitted changes detected. Please commit or stash your changes.${NC}"
    git status --short
    exit 1
fi

if ! git diff-index --quiet --cached HEAD --; then
    echo -e "${RED}Error: Staged changes detected. Please commit or reset them.${NC}"
    git status --short
    exit 1
fi

echo -e "${GREEN}✓ Repository is clean${NC}"
echo ""

# Extract current version from docker-compose.yml
echo -e "${YELLOW}Reading current version from ${COMPOSE_FILE}...${NC}"
if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}Error: ${COMPOSE_FILE} not found.${NC}"
    exit 1
fi

# Extract image tag like ghcr.io/alanjwade/fcxc-stats:v1.0.0
CURRENT_IMAGE=$(grep -m1 'image: ghcr.io/alanjwade/fcxc-stats:' "$COMPOSE_FILE")
if [ -z "$CURRENT_IMAGE" ]; then
    echo -e "${RED}Error: Could not find image line in ${COMPOSE_FILE}.${NC}"
    exit 1
fi

# Extract version tag (e.g., v1.0.0 from ghcr.io/alanjwade/fcxc-stats:v1.0.0)
CURRENT_VERSION=$(echo "$CURRENT_IMAGE" | sed 's/.*:v//' | awk '{print $1}')
if [ -z "$CURRENT_VERSION" ]; then
    echo -e "${RED}Error: Could not extract version from image tag.${NC}"
    echo "Image line: $CURRENT_IMAGE"
    exit 1
fi

echo "Current version: ${BLUE}v${CURRENT_VERSION}${NC}"
echo ""

# Parse version components
IFS='.' read -ra VERSION_PARTS <<< "$CURRENT_VERSION"
MAJOR="${VERSION_PARTS[0]}"
MINOR="${VERSION_PARTS[1]:-0}"
PATCH="${VERSION_PARTS[2]:-0}"

# Bump version
case "$BUMP_TYPE" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
echo -e "${YELLOW}New version will be:${NC} ${BLUE}v${NEW_VERSION}${NC}"
echo ""

# Update docker-compose.yml
echo -e "${YELLOW}Updating ${COMPOSE_FILE}...${NC}"
sed -i "s|image: ghcr.io/alanjwade/fcxc-stats:v[^[:space:]]*|image: ghcr.io/alanjwade/fcxc-stats:v${NEW_VERSION}|" "$COMPOSE_FILE"

# Verify the change
NEW_IMAGE=$(grep -m1 'image: ghcr.io/alanjwade/fcxc-stats:' "$COMPOSE_FILE")
echo "Updated image line: ${BLUE}${NEW_IMAGE}${NC}"
echo -e "${GREEN}✓ File updated${NC}"
echo ""

# Commit the change
echo -e "${YELLOW}Committing version bump...${NC}"
git add "$COMPOSE_FILE"
git commit -m "Bump fcxc-stats version to v${NEW_VERSION} for homelab01 deployment"

echo -e "${GREEN}✓ Changes committed${NC}"
echo ""

# Push to remote
echo -e "${YELLOW}Pushing to remote repository...${NC}"
if git push origin HEAD; then
    echo -e "${GREEN}✓ Changes pushed to remote${NC}"
else
    echo -e "${RED}Warning: Failed to push. You may need to push manually.${NC}"
fi

echo ""
echo -e "${GREEN}Version bump complete!${NC}"
echo "Next step: Run './deploy_homelab01.sh' to deploy the new version."
