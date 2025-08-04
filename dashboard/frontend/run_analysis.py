#!/usr/bin/env python3
"""
Analysis Dashboard Launcher
Run this script to start the Customer Behavior Analysis Dashboard
"""

import sys
import os

# Add the analysis directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'analysis'))

# Import and run the analysis main function
from main_analysis import main

if __name__ == "__main__":
    main() 