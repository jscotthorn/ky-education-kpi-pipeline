"""
Student Groups Configuration for QuantCrit-Aligned Analysis

This module defines the target student groups for Bayesian bright spots analysis,
following QuantCrit methodology which centers historically marginalized groups.

References:
- García, N. M., López, N., & Vélez, V. N. (2018). "QuantCrit: Rectifying quantitative
  methods through critical race theory." Race Ethnicity and Education, 21(2), 149-157.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class StudentGroup:
    """Configuration for a student group."""
    name: str              # KDE data value (e.g., "African American")
    slug: str              # URL/filename-safe identifier (e.g., "african_american")
    display_name: str      # Human-readable name for reports
    description: str       # Description for methodology documentation
    is_target_group: bool  # True if this is an equity-focus target group


# Master list of student groups for analysis
# Order matters: All Students first, then target groups in priority order
STUDENT_GROUPS: List[StudentGroup] = [
    StudentGroup(
        name="All Students",
        slug="all_students",
        display_name="All Students",
        description="School-wide average outcomes for all students",
        is_target_group=False,
    ),
    StudentGroup(
        name="Economically Disadvantaged",
        slug="economically_disadvantaged",
        display_name="Economically Disadvantaged",
        description="Students qualifying for free/reduced price lunch",
        is_target_group=True,
    ),
    StudentGroup(
        name="African American",
        slug="african_american",
        display_name="African American",
        description="Students identifying as African American/Black",
        is_target_group=True,
    ),
    StudentGroup(
        name="Students with Disabilities (IEP)",
        slug="students_with_disabilities",
        display_name="Students with Disabilities",
        description="Students with Individualized Education Programs (IEPs)",
        is_target_group=True,
    ),
    StudentGroup(
        name="Hispanic or Latino",
        slug="hispanic",
        display_name="Hispanic/Latino",
        description="Students identifying as Hispanic or Latino",
        is_target_group=True,
    ),
    StudentGroup(
        name="Homeless",
        slug="homeless",
        display_name="Homeless",
        description="Students experiencing homelessness",
        is_target_group=True,
    ),
    StudentGroup(
        name="English Learner",
        slug="english_learner",
        display_name="English Learners",
        description="Students classified as English Language Learners",
        is_target_group=True,
    ),
]


# Lookup dictionaries
STUDENT_GROUP_BY_NAME: Dict[str, StudentGroup] = {g.name: g for g in STUDENT_GROUPS}
STUDENT_GROUP_BY_SLUG: Dict[str, StudentGroup] = {g.slug: g for g in STUDENT_GROUPS}

# List of just the target groups (for QuantCrit-focused analysis)
TARGET_GROUPS: List[StudentGroup] = [g for g in STUDENT_GROUPS if g.is_target_group]

# All group slugs for iteration
ALL_GROUP_SLUGS: List[str] = [g.slug for g in STUDENT_GROUPS]
TARGET_GROUP_SLUGS: List[str] = [g.slug for g in TARGET_GROUPS]


def get_student_group(identifier: str) -> Optional[StudentGroup]:
    """
    Get StudentGroup by name or slug.

    Args:
        identifier: Either the KDE name (e.g., "African American")
                   or slug (e.g., "african_american")

    Returns:
        StudentGroup if found, None otherwise
    """
    return STUDENT_GROUP_BY_NAME.get(identifier) or STUDENT_GROUP_BY_SLUG.get(identifier)


def name_to_slug(name: str) -> str:
    """
    Convert KDE student group name to filename-safe slug.

    Args:
        name: KDE student group name (e.g., "African American")

    Returns:
        Slug (e.g., "african_american")

    Raises:
        ValueError if name not found in configuration
    """
    group = STUDENT_GROUP_BY_NAME.get(name)
    if group is None:
        raise ValueError(f"Unknown student group: {name}. "
                        f"Valid groups: {list(STUDENT_GROUP_BY_NAME.keys())}")
    return group.slug


def slug_to_name(slug: str) -> str:
    """
    Convert slug to KDE student group name.

    Args:
        slug: Filename-safe identifier (e.g., "african_american")

    Returns:
        KDE name (e.g., "African American")

    Raises:
        ValueError if slug not found in configuration
    """
    group = STUDENT_GROUP_BY_SLUG.get(slug)
    if group is None:
        raise ValueError(f"Unknown student group slug: {slug}. "
                        f"Valid slugs: {list(STUDENT_GROUP_BY_SLUG.keys())}")
    return group.name


def get_output_filename(base_name: str, student_group: str) -> str:
    """
    Generate output filename with student group suffix.

    Args:
        base_name: Base filename without extension (e.g., "graduation_analysis")
        student_group: Either KDE name or slug

    Returns:
        Filename with group suffix (e.g., "graduation_analysis_african_american.csv")
    """
    group = get_student_group(student_group)
    if group is None:
        raise ValueError(f"Unknown student group: {student_group}")
    return f"{base_name}_{group.slug}.csv"


def get_model_dirname(base_name: str, student_group: str) -> str:
    """
    Generate model output directory name with student group suffix.

    Args:
        base_name: Base model name (e.g., "graduation")
        student_group: Either KDE name or slug

    Returns:
        Directory name (e.g., "graduation_african_american")
    """
    group = get_student_group(student_group)
    if group is None:
        raise ValueError(f"Unknown student group: {student_group}")
    return f"{base_name}_{group.slug}"


# CLI helper for argparse
def add_student_group_argument(parser, default: str = "All Students"):
    """
    Add --student-group argument to argparse parser.

    Args:
        parser: argparse.ArgumentParser instance
        default: Default student group (KDE name or slug)
    """
    parser.add_argument(
        '--student-group',
        type=str,
        default=default,
        choices=ALL_GROUP_SLUGS + list(STUDENT_GROUP_BY_NAME.keys()),
        help=f"Student group to analyze. Choices: {ALL_GROUP_SLUGS}"
    )


if __name__ == "__main__":
    # Print configuration summary
    print("Student Groups Configuration")
    print("=" * 60)
    print(f"\nTotal groups: {len(STUDENT_GROUPS)}")
    print(f"Target groups (QuantCrit focus): {len(TARGET_GROUPS)}")
    print("\nAll groups:")
    for g in STUDENT_GROUPS:
        target_marker = " [TARGET]" if g.is_target_group else ""
        print(f"  {g.slug}: {g.name}{target_marker}")
