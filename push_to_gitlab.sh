#!/bin/bash

# Script to push to AWS GitLab once Midway SSH certificate is configured

echo "Checking git status..."
git status

echo "Pushing to GitLab..."
git push gitlab main

echo "If this fails, ensure your SSH key is Midway-signed and added to GitLab:"
echo "1. Get SSH key signed through AWS Midway process"
echo "2. Add signed certificate to GitLab profile"
echo "3. Run this script again"