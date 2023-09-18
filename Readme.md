# Python Code for UBC Micromet

Each application is in a separate folder, and each was created in a separate virtual environment.  For best results, you should use Python 3.10 or higher.

* Its is not explicitly required to use virtual environments, but it is good practice to ensure all dependencies are met and you don't corrupt your base Python installation
* The root folder of each application has a "requirement.txt" which lists the packages installed in the virtual environment
    * To install the packages, you can follow the steps listed below
    * It is best to do this in a dedicated virtual environment so you don't run into any conflicts with pre-existing installations in your main python environment.
    * See the instructions below to create a generic virtual environment with pip
## Create a virtual environment

### Using Visual Studio (VS) Code

If you have VS Code installed, with the python extension, you can:

1. Open the Micromet.py folder in VS Code
2. Hit ctrl + shift + p > and select "Create Python Environment"
    * Use Venv, not conda
3. You will be prompted to select dependencies to install
    * Select "requirements.txt" form the menu.  This will automatically install all required packages for you.

### Windows setup

This assumes you have Micromet.py installed in "C:\"

1. cd C:\Micromet.py\

2. py -m venv .venv

3. .\.venv\Scripts\activate

4. pip install -r .\requirements.txt

### macOS/Linux setup

This assumes you have Micromet.py installed in "/home/"

1. cd /home/Micromet.py/

2. python3 -m venv .venv

3. source ./.venv/bin/activate

4. pip install -r ./requirements.txt

# Creating a New Application

1. Create a new folder in Micromet.py/Python

2. Create a new virtual environment following the steps outlined above, **without** installing the requirements.txt

3. Write your code and install any necessary packages.  **Make sure** you work in a .venv

4. Write a README.md file :D

5. Push to you own branch of Micromet.py on github

# Updating Existing Applications

If any new packages are installed in a given application and you plan to push it to github, you can update the "requirements.txt" file with this command from within the application folder:

    pip freeze > requirements.txt

If it fails, try this instead:

    python -m pip freeze > requirements.txt