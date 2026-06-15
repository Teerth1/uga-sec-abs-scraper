# Dr. Honkanen Automation Pipeline

This workflow describes how to trigger an automated implementation plan when new instructions are received from Dr. Honkanen.

## Trigger
- New content added to `honkanen_instructions.txt` OR
- User provides a link to a new research note.

## Steps
1. **Analyze Instructions**: Antigravity reads the new instructions and identifies the core technical requirements.
2. **Generate implementation_plan.md**: A detailed technical plan is written to the artifacts directory.
3. **Create Visual Summary**: Antigravity uses `generate_image` to create a high-level, mobile-friendly summary of the plan.
4. **Notify User**: The user is alerted via `notify_user` with a link to the image and the plan.

// turbo
5. Run `python C:\tmp\interim_check.py` to ensure current state is stable before making changes.
