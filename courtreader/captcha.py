import urllib
import urllib2
import webbrowser
from bs4 import BeautifulSoup

def solve(opener, url):
    page = opener.open(url)
    soup = BeautifulSoup(page.read())
    opener.save_cookie()
    
    # Get the captcha image
    captcha_script_url = 'https://www.google.com/recaptcha/api/challenge'
    for script_tag in soup.find_all('script', {'src':True}):
        if script_tag['src'].startswith(captcha_script_url):
            captcha_script_url = script_tag['src']
    captcha_image_url = 'https://www.google.com/recaptcha/api/image?c='
    captcha_challenge = None
    page = urllib2.urlopen(captcha_script_url)
    for line in page:
        if line.strip().startswith('challenge'):
            captcha_challenge = line.split(':')[1].strip()[1:-2]
    
    # Show the captcha image to the user and ask them to solve it
    webbrowser.open(captcha_image_url + captcha_challenge)
    captcha_response = raw_input('Enter CAPTCHA: ')
    
    # Submit our captcha solution
    data = urllib.urlencode({
        'recaptcha_challenge_field': captcha_challenge,
        'recaptcha_response_field': captcha_response,
        'accept': 'Accept',
        'pageName': 'landingCaptchaVerificationPage',
        'showCaptcha': True})
    page = opener.open(url, data)
    
    # Did we pass the captcha?
    if 'The reCAPTCHA challenge failed' in page.read():
        raise Exception('Captcha Failed')