%sh
pip uninstall typing_extensions --yes

pip install -r databricks_requirements.txt

# Update package information
sudo apt-get update

# Function to check and install a Debian package
install_package() {
    local url="$1"
    local package_name="$(basename $url)"

    # Check if the package is already installed (dpkg-query returns 0 if the package is installed)
    if ! dpkg -s "${package_name%%_*}" &>/dev/null; then
        echo "Package $package_name is not installed. Proceeding with download and installation."
        # Download the package if it is not installed
        if wget -N "$url"; then
            # Install the downloaded package
            sudo dpkg -i "$package_name"
        else
            echo "Failed to download $package_name."
            return 1
        fi
    else
        echo "Package $package_name is already installed."
    fi
}

# Install required packages
install_package "http://ftp.de.debian.org/debian/pool/main/f/fonts-liberation/fonts-liberation_1.07.4-11_all.deb"
install_package "http://ftp.de.debian.org/debian/pool/main/m/mesa/libgbm1_20.3.5-1_amd64.deb"
install_package "http://ftp.de.debian.org/debian/pool/main/n/nspr/libnspr4_4.29-1_amd64.deb"
install_package "http://ftp.de.debian.org/debian/pool/main/n/nss/libnss3_3.61-1+deb11u1_amd64.deb"

# Attempt to fix any broken dependencies
sudo apt-get -f install -y

# Check for Google Chrome and install if not present
if ! which google-chrome &>/dev/null; then
    echo "Google Chrome is not installed. Proceeding with installation."
    if wget -N "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb"; then
        sudo dpkg -i "google-chrome-stable_current_amd64.deb"
        # Fix any broken dependencies after installing Chrome
        sudo apt-get install -f -y
    else
        echo "Failed to download Google Chrome."
    fi
else
    echo "Google Chrome is already installed."
fi