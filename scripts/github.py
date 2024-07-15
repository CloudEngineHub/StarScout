import logging
import stscraper as scraper

from typing import Optional

from scripts import GITHUB_TOKENS

def get_repo_id(repo: str) -> Optional[str]:
    owner, name = repo.split("/")
    strudel = scraper.GitHubAPIv4(",".join(GITHUB_TOKENS))

    try:
        result = strudel(
            """
            query ($owner: String!, $name: String!) {
                repository(owner: $owner, name: $name) {
                    id
                }
            }
            """,
            owner=owner,
            name=name,
        )
        return result
    except Exception as ex:
        logging.error(f"Error fetching repo {repo}: {ex}")
        return None