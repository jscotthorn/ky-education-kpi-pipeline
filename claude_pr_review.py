#!/usr/bin/env python3

import os
import json
import time
import subprocess
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
import requests
import logging
import signal
import sys

# Configuration
CONFIG = {
    "repo_path": "/Users/scott/Projects/equity-etl/ky-education-kpi-pipeline",  # Your repo path
    "data_path": "/Users/scott/Projects/equity-etl/ky-education-kpi-pipeline/data",  # Path to data directory
    "github_token": os.environ.get("GITHUB_TOKEN", "your_token_here"),
    "repo_owner": "jscotthorn",
    "repo_name": "ky-education-kpi-pipeline",
    "check_interval": 300,  # seconds
    "base_branch": "develop",  # Changed from "main" to "develop"
    "required_label": "codex",  # Only process PRs with this label
    "max_prs_to_review": None,  # Set to a number to limit reviews (e.g., 5), None for unlimited
    "claude_path": "/Users/scott/.claude/local/claude",  # Full path to claude executable
    "review_dir": Path.home() / ".pr_reviewer",
    "processed_prs_file": Path.home() / ".pr_reviewer" / "processed_prs.json"
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GitHubPRReviewer:
    def __init__(self, config):
        self.config = config
        self.processed_prs = self.load_processed_prs()
        self.setup_directories()
        self.total_prs_reviewed = 0
        self.caffeinate_process = None
        
    def setup_directories(self):
        """Create necessary directories"""
        self.config["review_dir"].mkdir(exist_ok=True)
        (self.config["review_dir"] / "pr_clones").mkdir(exist_ok=True)
        
    def start_caffeinate(self):
        """Start caffeinate to keep macOS awake"""
        if sys.platform == "darwin":  # macOS
            try:
                logger.info("Starting caffeinate to keep system awake...")
                self.caffeinate_process = subprocess.Popen(
                    ["caffeinate", "-d", "-i", "-s"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                logger.info("System will stay awake while the script is running")
            except Exception as e:
                logger.warning(f"Could not start caffeinate: {e}")
                
    def stop_caffeinate(self):
        """Stop caffeinate process"""
        if self.caffeinate_process:
            try:
                self.caffeinate_process.terminate()
                self.caffeinate_process.wait(timeout=5)
                logger.info("Stopped caffeinate")
            except Exception as e:
                logger.warning(f"Error stopping caffeinate: {e}")
                if self.caffeinate_process:
                    self.caffeinate_process.kill()
        
    def setup_directories(self):
        """Create necessary directories"""
        self.config["review_dir"].mkdir(exist_ok=True)
        (self.config["review_dir"] / "pr_clones").mkdir(exist_ok=True)
        
    def load_processed_prs(self):
        """Load list of already processed PRs"""
        if self.config["processed_prs_file"].exists():
            with open(self.config["processed_prs_file"], 'r') as f:
                return json.load(f)
        return {}
        
    def save_processed_prs(self):
        """Save list of processed PRs"""
        with open(self.config["processed_prs_file"], 'w') as f:
            json.dump(self.processed_prs, f, indent=2)
            
    def get_open_prs(self):
        """Fetch open PRs from GitHub API with the required label"""
        headers = {
            "Authorization": f"token {self.config['github_token']}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        url = f"https://api.github.com/repos/{self.config['repo_owner']}/{self.config['repo_name']}/pulls"
        params = {"state": "open"}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            all_prs = response.json()
            
            # Filter PRs with the required label
            labeled_prs = []
            for pr in all_prs:
                labels = [label['name'] for label in pr.get('labels', [])]
                if self.config['required_label'] in labels:
                    labeled_prs.append(pr)
                    
            logger.info(f"Found {len(labeled_prs)} PRs with '{self.config['required_label']}' label")
            return labeled_prs
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch PRs: {e}")
            return []
            
    def run_command(self, cmd, cwd=None, shell_escape=True):
        """Run shell command and return output"""
        try:
            # For complex commands with quotes, use list format
            if shell_escape and cmd.startswith("claude code"):
                # Split the command properly to handle quotes
                import shlex
                cmd_list = shlex.split(cmd)
                result = subprocess.run(
                    cmd_list,
                    capture_output=True,
                    text=True,
                    cwd=cwd,
                    check=True
                )
            else:
                result = subprocess.run(
                    cmd, 
                    shell=True, 
                    capture_output=True, 
                    text=True, 
                    cwd=cwd,
                    check=True
                )
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {cmd[:100]}...")
            logger.error(f"Error: {e.stderr}")
            raise
            
    def create_claude_prompt(self, pr_info, diff_content, files_changed):
        """Create a detailed prompt for Claude"""
        prompt = f'''Review this GitHub PR #{pr_info['number']} "{pr_info['title']}" by {pr_info['user']['login']}. 

Description: {pr_info.get('body', 'No description provided')[:200]}

Files changed ({len(files_changed)}): {', '.join(files_changed[:10])}{' and more' if len(files_changed) > 10 else ''}

Analyze the diff below for bugs, security issues, performance problems, and code quality. Check if tests are adequate and documentation is updated. 

Run any ETL pipelines and/or tests that were touched by this PR. Allow for a long timeout (up to 10 minutes) for individual etl pipelines, and 30 minutes if testing the full etl pipeline (only needed when changes affect the full pipeline or build process).

Structure your review with sections: Summary, Code Quality, Potential Issues, Testing, Breaking Changes, Suggestions, and Overall Assessment.

Diff:
{diff_content[:8000]}
'''
        
        return prompt

    def review_pr(self, pr):
        """Review PR using temporary clone"""
        pr_number = pr['number']
        branch_name = pr['head']['ref']
        sha = pr['head']['sha']
        
        logger.info(f"Starting review of PR #{pr_number}: {pr['title']}")
        
        # Create temporary directory for clone
        temp_clone_dir = None
        
        try:
            # Create temporary directory
            temp_clone_dir = Path(tempfile.mkdtemp(prefix=f"pr_{pr_number}_"))
            logger.info(f"Created temporary directory: {temp_clone_dir}")
            
            # Clone repository
            logger.info("Cloning repository...")
            repo_url = f"https://github.com/{self.config['repo_owner']}/{self.config['repo_name']}.git"
            self.run_command(
                f"git clone {repo_url} .",
                cwd=temp_clone_dir
            )
            
            # Fetch PR branch
            logger.info(f"Fetching PR branch: {branch_name}")
            self.run_command(
                f"git fetch origin pull/{pr_number}/head:pr_{pr_number}",
                cwd=temp_clone_dir
            )
            self.run_command(f"git checkout pr_{pr_number}", cwd=temp_clone_dir)
            
            # Create symlink to data directory
            data_symlink = temp_clone_dir / "data"
            if data_symlink.exists():
                # Remove existing data directory if it exists
                if data_symlink.is_symlink():
                    data_symlink.unlink()
                else:
                    shutil.rmtree(data_symlink)
            
            logger.info(f"Creating symlink: {data_symlink} -> {self.config['data_path']}")
            data_symlink.symlink_to(Path(self.config['data_path']))
            
            # Get list of changed files
            files_changed = self.run_command(
                f"git diff --name-only origin/{self.config['base_branch']}...HEAD",
                cwd=temp_clone_dir
            ).strip().split('\n')
            
            # Get diff
            diff_content = self.run_command(
                f"git diff origin/{self.config['base_branch']}...HEAD",
                cwd=temp_clone_dir
            )
            
            # Create Claude prompt
            prompt = self.create_claude_prompt(pr, diff_content, files_changed)
            
            # Run Claude review
            logger.info("Running Claude review from temporary clone...")
            logger.info(f"Working directory: {temp_clone_dir}")
            logger.info(f"Prompt length: {len(prompt)} characters")
            
            # Use the configured claude path
            claude_cmd = self.config.get("claude_path", "claude")
            logger.info(f"Using Claude command: {claude_cmd}")
            
            # Write prompt to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                f.write(prompt)
                prompt_file = f.name
            
            try:
                # Verify prompt file contents
                with open(prompt_file, 'r') as f:
                    prompt_content = f.read()
                logger.info(f"Prompt length: {len(prompt_content)} characters")
                logger.info(f"First 100 chars of prompt: {prompt_content[:100]}...")
                
                # Run Claude with stdin
                logger.info(f"Running command: {claude_cmd} (with stdin, 30-minute timeout)")
                logger.info(f"Working directory: {temp_clone_dir}")
                
                process = subprocess.Popen(
                    [claude_cmd],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    cwd=temp_clone_dir
                )
                
                stdout, stderr = process.communicate(input=prompt_content, timeout=1800)  # 30 minutes
                
                result = subprocess.CompletedProcess(
                    args=[claude_cmd],
                    returncode=process.returncode,
                    stdout=stdout,
                    stderr=stderr
                )
                
                logger.info(f"Claude command return code: {result.returncode}")
                logger.info(f"Claude stdout length: {len(result.stdout)} characters")
                logger.info(f"Claude stderr length: {len(result.stderr)} characters")
                
                if result.returncode != 0:
                    logger.error(f"Claude command failed with return code {result.returncode}")
                    logger.error(f"stderr: {result.stderr}")
                    raise subprocess.CalledProcessError(result.returncode, [claude_cmd], result.stdout, result.stderr)
                
                review_output = result.stdout
                
                logger.info(f"Claude review completed, output length: {len(review_output)} characters")
                logger.info(f"First 200 chars of output: {review_output[:200]}...")
                
                if not review_output.strip():
                    raise Exception("Claude returned empty output")
                    
            finally:
                # Clean up prompt file
                os.unlink(prompt_file)
            
            # Create and post review comment
            try:
                logger.info("Formatting GitHub comment...")
                review_comment = self.format_github_comment(pr, review_output)
                logger.info(f"Comment formatted, length: {len(review_comment)} characters")
            except Exception as e:
                logger.error(f"Error formatting GitHub comment: {e}")
                raise Exception(f"Failed to format GitHub comment: {e}")
            
            try:
                logger.info("Posting comment to GitHub...")
                self.post_github_comment(pr_number, review_comment)
                logger.info("Comment posted successfully")
            except Exception as e:
                logger.error(f"Error posting GitHub comment: {e}")
                raise Exception(f"Failed to post GitHub comment: {e}")
            
            # Mark as processed
            self.processed_prs[str(pr_number)] = {
                "reviewed_at": datetime.now().isoformat(),
                "sha": sha,
                "title": pr['title']
            }
            self.save_processed_prs()
            
            # Increment review counter
            self.total_prs_reviewed += 1
            
            logger.info(f"Successfully reviewed PR #{pr_number} ({self.total_prs_reviewed} total reviewed)")
            
        except Exception as e:
            logger.error(f"Failed to review PR #{pr_number}: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            # Post error comment
            error_comment = self.format_error_comment(pr, str(e))
            try:
                self.post_github_comment(pr_number, error_comment)
            except Exception as posting_error:
                logger.error(f"Failed to post error comment: {posting_error}")
            raise
            
        finally:
            # Clean up temporary directory
            if temp_clone_dir and temp_clone_dir.exists():
                logger.info(f"Cleaning up temporary directory: {temp_clone_dir}")
                try:
                    shutil.rmtree(temp_clone_dir)
                    logger.info("Temporary directory cleaned up successfully")
                except Exception as cleanup_error:
                    logger.error(f"Failed to clean up temporary directory: {cleanup_error}")
                
    def format_github_comment(self, pr, claude_output):
        """Format Claude's review for GitHub comment"""
        return f"""## 🤖 Claude AI Code Review

**PR**: #{pr['number']} - {pr['title']}  
**Reviewed at**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}  
**Commit**: {pr['head']['sha'][:7]}

---

{claude_output}

---

<details>
<summary>📋 Review Metadata</summary>

- **Model**: Claude (via CLI)
- **Label Trigger**: `{self.config['required_label']}`
- **Base Branch**: `{pr['base']['ref']}`
- **Head Branch**: `{pr['head']['ref']}`
- **Review ID**: `{pr['head']['sha'][:7]}-{int(time.time())}`

</details>

<sub>This is an automated review generated by Claude AI. Please consider it as supplementary to human code review. [Learn more](https://github.com/{self.config['repo_owner']}/{self.config['repo_name']}/wiki/AI-Code-Reviews)</sub>"""

    def format_error_comment(self, pr, error_message):
        """Format error message for GitHub comment"""
        return f"""## 🤖 Claude AI Code Review - Error

**PR**: #{pr['number']} - {pr['title']}  
**Attempted at**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

❌ **Review failed due to an error:**

```
{error_message}
```

The automated review could not be completed. Please review this PR manually or check the error above.

<sub>This is an automated message from the Claude AI reviewer.</sub>"""

    def post_github_comment(self, pr_number, comment_body):
        """Post review as a comment on the PR"""
        headers = {
            "Authorization": f"token {self.config['github_token']}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        # Check if we've already commented on this PR
        existing_comments_url = f"https://api.github.com/repos/{self.config['repo_owner']}/{self.config['repo_name']}/issues/{pr_number}/comments"
        
        try:
            # Get existing comments
            response = requests.get(existing_comments_url, headers=headers)
            response.raise_for_status()
            existing_comments = response.json()
            
            # Look for our previous comment
            bot_comment_id = None
            for comment in existing_comments:
                if "🤖 Claude AI Code Review" in comment.get('body', ''):
                    bot_comment_id = comment['id']
                    break
            
            if bot_comment_id:
                # Update existing comment
                update_url = f"https://api.github.com/repos/{self.config['repo_owner']}/{self.config['repo_name']}/issues/comments/{bot_comment_id}"
                response = requests.patch(
                    update_url,
                    headers=headers,
                    json={"body": comment_body[:65000]}  # GitHub comment limit
                )
                logger.info(f"Updated existing review comment on PR #{pr_number}")
            else:
                # Create new comment
                response = requests.post(
                    existing_comments_url,
                    headers=headers,
                    json={"body": comment_body[:65000]}
                )
                logger.info(f"Posted new review comment on PR #{pr_number}")
                
            response.raise_for_status()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to post comment: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            
    def is_pr_processed(self, pr):
        """Check if PR has already been reviewed"""
        pr_number = str(pr['number'])
        if pr_number in self.processed_prs:
            # Check if PR has been updated since last review
            last_sha = self.processed_prs[pr_number].get('sha')
            if last_sha != pr['head']['sha']:
                logger.info(f"PR #{pr_number} has been updated since last review")
                return False
            return True
        return False
        
    def run(self):
        """Main loop"""
        logger.info("Starting GitHub PR Auto-Reviewer")
        logger.info(f"Repository: {self.config['repo_owner']}/{self.config['repo_name']}")
        logger.info(f"Filtering by label: '{self.config['required_label']}'")
        logger.info(f"Check interval: {self.config['check_interval']} seconds")
        
        if self.config['max_prs_to_review']:
            logger.info(f"Max PRs to review: {self.config['max_prs_to_review']}")
        else:
            logger.info("Max PRs to review: Unlimited")
            
        # Start caffeinate to keep system awake
        self.start_caffeinate()
        
        try:
            while True:
                # Check if we've hit the max PR limit
                if (self.config['max_prs_to_review'] and 
                    self.total_prs_reviewed >= self.config['max_prs_to_review']):
                    logger.info(f"Reached maximum PR review limit ({self.config['max_prs_to_review']}). Exiting...")
                    break
                    
                try:
                    logger.info("Checking for new PRs with 'codex' label...")
                    prs = self.get_open_prs()
                    
                    new_prs_count = 0
                    for pr in prs:
                        # Check limit again in case we're processing multiple PRs
                        if (self.config['max_prs_to_review'] and 
                            self.total_prs_reviewed >= self.config['max_prs_to_review']):
                            logger.info("Reached review limit during processing")
                            break
                            
                        if not self.is_pr_processed(pr):
                            new_prs_count += 1
                            logger.info(f"Found new/updated PR: #{pr['number']} - {pr['title']}")
                            try:
                                self.review_pr(pr)
                            except Exception as e:
                                logger.error(f"Failed to review PR #{pr['number']}: {e}")
                                continue
                    
                    if new_prs_count == 0:
                        logger.info("No new PRs to review")
                    
                    # Exit if we've hit the limit
                    if (self.config['max_prs_to_review'] and 
                        self.total_prs_reviewed >= self.config['max_prs_to_review']):
                        break
                                
                    logger.info(f"Check complete. Sleeping for {self.config['check_interval']} seconds...")
                    time.sleep(self.config['check_interval'])
                    
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error in main loop: {e}")
                    time.sleep(60)  # Wait a minute before retrying
                    
        finally:
            # Always stop caffeinate when exiting
            self.stop_caffeinate()
            logger.info(f"Total PRs reviewed: {self.total_prs_reviewed}")

if __name__ == "__main__":
    # Handle command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='GitHub PR Auto-Reviewer with Claude')
    parser.add_argument('--max-prs', type=int, help='Maximum number of PRs to review before exiting')
    args = parser.parse_args()
    
    # Override config with command line argument if provided
    if args.max_prs:
        CONFIG['max_prs_to_review'] = args.max_prs
    
    # Validate configuration
    if CONFIG["github_token"] == "your_token_here":
        logger.error("Please set your GitHub token in the GITHUB_TOKEN environment variable")
        exit(1)
        
    if not Path(CONFIG["repo_path"]).exists():
        logger.error(f"Repository path does not exist: {CONFIG['repo_path']}")
        exit(1)
        
    if not (Path(CONFIG["repo_path"]) / ".git").exists():
        logger.error(f"Path is not a git repository: {CONFIG['repo_path']}")
        exit(1)
        
    # Set up signal handler for clean exit
    def signal_handler(sig, frame):
        logger.info(f"Caught signal {sig}, cleaning up...")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    reviewer = GitHubPRReviewer(CONFIG)
    reviewer.run()