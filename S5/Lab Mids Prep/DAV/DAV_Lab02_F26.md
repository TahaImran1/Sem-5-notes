**National University of Computer and Emerging Sciences**

		            **Laboratory Manual \# 02**  
			  **Data analysis and visualization**

| Course Instructor | Muhammad Amir Iqbal  |
| :---- | :---- |
| Lab Instructor | Muhammad Hashir Mohsineen |
| Date | 31-Aug-26 |
| Semester | Fall- 26 |

**Objectives:**

* Fetch JSON data from public REST APIs using the requests library.  
* Inspect and map the schema of nested JSON payloads.  
* Traverse complex, deeply nested data structures using recursive functions.  
* Flatten hierarchical JSON data into tabular formats using pandas.json\_normalize.

**Software and tools:**

* Pandas   
* Requests  
* json

| import pandas as pdimport requestsimport json |
| :---- |

**Learning Material:**

* [https\://pandas.pydata.org/docs/reference/api/pandas.json\_normalize.html](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html)

**APIs used in this Lab:**

1. [https\://randomuser.me/](https://randomuser.me/)  
2. [https\://dummyjson.com/docs/products](https://dummyjson.com/docs/products)

| response \= requests.get('api')data \= response.json()df \= pd.DataFrame(data\['results'\]) |
| :---- |

**Task 1:**

1. Write a script that sends a GET request to https\://randomuser.me/api/.  
2. Check response.status\_code before proceeding. If the request fails, print an error message and exit instead of letting the script crash.  
3. Load the JSON body with response.json().  
4. Write a function that prints, for every top-level key:  
* the key name,  
* the Python type of its value (type(value).\_\_name\_\_),  
* if the value is a list, also print its length and the type of its first element (if any).

**Task 2:**

1. Implement a recursive Python function extract\_all\_keys.  
2. The function must traverse the entire fetched JSON object (from Task 1).  
3. It should extract and return a set of all unique keys present in the payload, regardless of how deeply nested they are within dictionaries or lists.  
4. Print the final sorted list of unique keys.

**Task 3:**  
Convert a deeply nested API response into a clean, analysis-ready DataFrame. 

1. Fetch 10 users from the Random User API (https\://randomuser.me/api/?results=10).  
2. Extract the results list and pass it to pd.json\_normalize().  
3. Use the sep="\_" argument so nested fields become columns like location\_city, login\_username, dob\_age, etc.  
4. Print df.shape, df.columns.tolist(), and df.head().  
5. Select and display only a subset of "useful" columns for a contact list: name\_first, name\_last, email, phone, location\_city, location\_country.  
6. Error handling requirement: Wrap the column-selection step so that if the API changes and a column is missing, your script does not crash.

![][image1]

**Task 4:**

1. Connect to https\://dummyjson.com/products?limit=30 and load the products array from the JSON response.  
2. Use pandas.json\_normalize() to create a DataFrame.   
3. Create a new column named discounted\_price. Calculate it using the formula: price \* (1 \- (discountPercentage / 100)). Round the result to 2 decimal places.  
4. Filter the DataFrame to retain only products that meet all the following criteria:  
   1. category is either 'beauty' or 'smartphones'  
   2. rating is greater than or equal to 4.5  
   3. stock is strictly greater than 0  
5. Export:  
   1. Save the filtered, transformed products DataFrame to a CSV file named filtered\_products.csv.  
   2. Save the aggregated summary DataFrame to a CSV file named category\_summary.csv.

---

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAnAAAABXCAYAAACEJXgEAAAcvElEQVR4Xu1cS44cuZLUGV4PHjA90zNVy3eI3ghd0D20FaAEdAutBS3UdQHNAQq1USu3fYOCdlLdQduXE/w4ae50Rv4i8msGGKqSwWCQTv9YsFJ6sSIIgiAIgiDOCi9sA0EQBEEQBHHaoIAL+LJY3d7eRt6/Sj8XX2ynC8P3+9VdWPOr+9WzvUYQR8WzisOr9VPG6GRYvps7ryefnW/8BFnHPHgexr5b3X+37VNBxzWxPyjgJEnuIeDmdfqZsHNxWK4W+T6CmAcUcBE7xyhhMYuAG/bn9t0yfziCgBP/KHPYF9MLuOdPdyA4z1PA6TWcFijg5PRt5yCY3ukPgl2LA9xHEIfATn56Cdg1RokGswi4UDt2rhsTYO/aZTF9LZv3xPAwOOU1HF/AhbeYkKDgz5gJ+aQnszhV7ieq+PZ2MfQUlaydD0/W7j75KVCuJ97lN4Q8Tn5W2sD0HNnM1EfPcatAwuScn1OTC44La8oJA9def4d+5lTRhSkOah3DWAn1jSmuH/ZIz5e4dnh+mHyk+pDyp+x39T70VemXxrp4EdPLgaM5Que38kIFRd3LidXW0xbqU4cVcDrfLbBnveb5aM7x2m+DLZPPVptiDk+1I6K31x3oelM/27gJ13F+AdvvsRVwnTWY2hzWU+uErAlrR+rzbOLa1qkS302NH1tHO5fYWuzW1lUZC33CjzNnDRCTWgPkmjn5qeg4XtiGgyM69J3ayGVuVwlKHCgbWImzV22AxXHV/X6g6D6+gEvPWqz+rwnaKQQcrj2vcWztxlaK4fnNvR2hZQScvWe50kFAAUf00BaT5K/hd4zNO5Pkg/9oH5MXrWsUcDquVYHr5Aht81wAJUbxHk+I4FhXAC3glo3tlqlXIwaC3TwfHRdwZpxI2Dev3jnQz10v4KROLdPdO8QMCrj+Gpr24Tm2TeyBfayAszWnzNep8b212OeGfna/AmXfxwWcjTNnDRCTdk8CZF8OVRtPRMDJ5kAQGLFgRZXc24iQTtCVeyzgjTXAE3AV3huJfWvZEGbuapyxtYNADffHe2Esb92usDTPt/eoAoLzhPsIwk/0KYHJz4CQ2OQUXJJc+Gzf3JOvOom+k8AvAioHgq1MjKJNQpxjkSifTT5LexNyVX+frgFYrNEXA+Qz+mVFz0dXOh9D7bLjqM+9emfR1IcEFAt2r/Fz6OeOO4pag8bW0NqoFWPiV2q+Jq6xX0D5rOqu+K3/stGdC9itfl4n4JxaDP0i3PqX9/FLvubV25nwwjYcHK5Dm6PW8Dbhiao1As5urIutBFyCBLScFkwr4Oraq/NuL+DWrl09v33z0utJb6wxuFwHJq4X5k8IAEzQPQGHsRfbr1XAQYEquWuNgKsxDjG7RsBtnacuBFbAWeFV2q2vgT3L9Q0FnPblmj/bepeHQMjem9oyKuDkWfkkaXu0As5bQ2OjFcZ6tWfAOgGHPuzX3fUCzs4l2a32t+vAecq8/Vqc7l8v4FZxzumvDTtogT3wwjYcHB2HFqNVOps7IuCKc8P9LrYQcMWpM8URSts2yrsr4NasfY2Aa9e9JkHk53v36HnkOZTEot+eiOtF46/gU+sEnPhS4dUKOG0HFWuOgPPuiegKuP4+XQNQwCWxoG2R7GBOKYN9IN8VooCLbWFPUIw5p51i60698+DlX1fA5esBWKO2R/si763Ba1efb1sBl+514hpp7JpHGBVw3lwaP3fEmJ1rrxare9AfQq1VyD514Jh6YRsOjp5DQ+Dcfeqo8zEBt7KiZFoBhydc5RkTCTi5FtbdzGedgAswIs5NEJ7t4JmYLAJRrOlkSBAmMWafQh/pCbh63yJ9x/JaBVwoPLb4jgm4lclvVlQ4Aq6555JtamBzlhIPqhiDcMn2kXujf6KtY84O16yAW6n6pezcq3cd1PhwBFyJk3Q9ojwX/2HGpjB/TeqtwYq7V/AduME2YY6lNigf1nFt61TBFgLOm4u2i7FvsD/MM/y+TsCpNdhaCwhjrf3L18R4YRsuGSpoIzui7tKAgZjXPZY0CII4IFRRJ64OTX4eF3U9XHx+F/F1UvUbxOI2BzgT4aoEHEEQxMmBAo6YACJs+JeRQ6IKuGPELwUcQRAEQRDEmYECjiAIgiAI4sxAAUcQBEEQBHFmoIAjCIIgCII4M1DAEQRBEARBnBko4AiCIAiCIM4MGwu4v/76iyRJkiRJktyBU2NjAffj579JkiRJkiTJHTg1KOBIkiRJkiRn5tSggCNJkiRJkpyZU4MCjiRJkiRJcmZODQo4kiRJkiTJmTk1KOBIkiRJkiRn5tSggCNJkiRJkpyZU4MCjiRJkiRJcmZOjf0E3MNi9dvNreLfts8Z8u8Pf6x+//CjaSfPgMEn7/5s20cY9vu3t1+b9sKnP1e/3yxWn237zLSxZa8fk5cQH2LXspa4z569f6ze35m+HX5+m+8Hf4r+JeNCe+mrnvV19drOK7c384W+1j+8edS5/LF6/1TbyCMw+NqQp+arl18Pnq8uh/PZbmrsJeAkSbx+CJ9TMjn5xJAL/FjgKAE3BNrnmNjPP+nF5L2luDk7XoSAS7Gk236Mz/HAXCdkTp1hz9PvydYhh/394c+6xw91v9E/Xo/lgeJ7SfClvPhvyDepHftKu/T98fBnGt/knL8/LPJz6nxL39D+VH+XOA9j4zzCvb8PaxxdA3kYbiTgwl7vulfziZDL53y2mxp7Cbj09lcTXRB0557YA3kCdz6Ul4iwX3jS8fpBF9FAEWGlXy7KUqD1CwlQBJycOHunK6Et91PXcnu5N8/Vvkhg7Mjv5RTlbrF6/fbP+BnnVk9fapKvc6tiAU96RNymZ4RiLuuppzl2/p5dMD5wD6St0ju9gpMjY0vpm2yT711b6LZlGFcEXE/A1ySOdrd7gMS8gb+r/sO+h7XYHNPaLs3RK95+nv2R+5r7gp+pte0jCsjJuJGA24fziZDLp459yZNhrzCXyt5J/rC52OPUmFTA2aR0cpSiueYU7uTXQUa6hRdO4Ox1TziVflmgxN+tb8if1opIy0HanPbBCQsW0uGeMo98vy2kryWOILGre8JzVNKHUxs4ISxjlrkNz+mKOrkH4/hrGRftYk+EUKjIPK1gludJ3/fxp25DMRIFXBEhNRli/2kYbF+FdruPqU1+xzWOih/3BE6EVWYQ89C32Uvo5xf4jrB7wlM+uN6MQwF3Emz2paXEX5PjNiIF3O7UAk7lRsilKgfepLzq1g/g1LguAdcQnby+ldt1lOLaY/OWe6JU80xFbNN14Xdtxhz0oHTEmCrGRvCImLJ/DlVCz7mu26A453acUxirebYUbbg//I6nOeX5w/yT72Hhl8Qx/JQxH9JJTh0ThNvbP/0iD/ND4WRPdDxxZvuln1oM2Lhxi5TdM+hTbGeebeexN+2+5Tmk9WQfN8+TJI02LW/j2DesT0RvbPtq9gFyTu4ryT8xi1drN3ymEeQSk16f+DsF3HFpfT7Qxkb0SdgryDlhD5sxC8FnzT7XnENuRy3geprGzZNe/QBOjb0EnCSPlKySI51qYijFQQXTLgKuFtbS79gCrnl+DWr755uegMOCr2zlrSsnm56THoMogLCYiSBCcVDuAeHQs0ukCUoUXvhMSZ7vn9pTEByvJuTQL4wLfhieFedST2U+Bz8L46NfPmjhqObydlGKtBU/YoddBZzMqQq4frKydnHbjiHg4rz9ZyFRpJc12vmbMdXJV7zPnK49BOHdiigv6es8tL4ge4XG5rJmnuRBWXzc+D3GYvSXKO6zTzxZAb5oBaAZcxN/IXv0BZzNE377eE6cGnsJuDhxUf+ZbWI7BlNhxLYSOJiAn+BLwJjUjXDR/4Dhay2seXPtxh6aTZKW+ecigv2KuInX6rrKF6N/VoEh/VWxjft8egWgzNMKuLBf4Ttkxh8CpWh2A1CE1zoBZ67/PgioJMLqdQxofKMOz379FudWT/iSrcPv+aRF+Vj1Q6QVblqc6dO/jQSc7DUWFOgT7m18v/hdsKX9E6puswlSxsA9mSO+6rOqoHn/4WvtU3IDnLj+7Kw3065FfLLGkG//QOnz+UMtwF5MqueVvrl/7otshSEF3OH4oynk2u/S3tSXvZCr6p5GXwt7Vf4Bi9z3x3DN2UP1cvG1eTa5Kavt+nnyx/kLuDJ5EXCqcB6TyYjYVue5GAKizvnXX/6x+sd/5/mXTUiJ9p//+x+rX4f2EEDYTwpr+lPV8QVNW+AgSQ9B/V+//Fb25/3ghGFNkhhkXe+fUsGI6xscU/p/Hsb+9V//yvY67jot0fdqwOiCG6iKbhQi6R5ZSzcA1wm4Mpa2CxbeyBEBJ0K6fC5tbcG2LH6Y9yp86b8Va3Vfwzxfv932BM73cfscZQf14lCfX22ifU3GPKSAC/MW2ylxBraqfcGGo/MAWzSF2K4X7Wa+EN3YS/ct42PuvQF/AR9vCwkF3OHYiijr4yH/qn2N/M+m7ddf/mf1z/z760G42+uJuK/ts8lN6Qs4m0svQsCdJn0B1/a7EJrTtJTwmaSFRXA51+bgWAA3DHvXefGxCdpen5vzCCeSvBa2Iso7SVUncNvkDvLsODUuVMAF1v/40p4ekNdAObU44N7nkzM5TRmlnJKcsECigCPJ/aj+8Vc3F/knt+TlcWpcsIAjSZIkSZI8DU4NCjiSJEmSJMmZOTUo4EiSJEmSJGfm1KCAI0mSJEmSnJlTgwKOJEmSJElyZk4NCjiSJEmSJMmZOTU2FnA/f/4kSZIkSZIkd+DUoIAjSZIkSZKcmVODAo4kSZIkSXJmTg0KOJIkSZIkyZk5NSjgSJIkSZIkZ+bUoIAjSZIkSZKcmVPjqgTcm5uXq49PbTtJkiRJkuScnBpXJeAo3kiSJEmSPAanxiQC7tuHl6ubm5vVzd3H5trp8FF/fvq4ennzJra/Geb+5sH2J8+J0Qff5j0e9nYOsR6eYduulS8/fGvaSJI8InNNe7TtF8rHtzdN2+4MOmCCv9A9vKl1yOHU2E/ARYe5SeLthAVcEZiReZOKgGv7nzXXONClUgm4mXhoAVd9NtFePyYvQsCp/JVygc4VieHlrtdex/u2+ninr4cxsZimMTDn1Ht8e6aXyzRW9T01lyHnfmvugz4mJlL7BIWKjPx4Z20Z9uxIIuqcBdww9+LT2+bxUPM6cbA5r1HACSURnrCAaxKkOoGrG+cl3xgUYWPytTqOJNgJNn4qdhwIkz7ONbzFvPzwWIpEvdYWj9C32scWsMMSC2/YJ3sCJ3uHc5YAl77tmttx8Zko4MbsWQs5jN2IBbSvfl7ak2917ndvVm/efoyf0ea4P8V/y94ncSDjlefkGE3P+JjGiPfU+ag1Dtfkfnw2xpNcb2IM5qGvw9rBV8Pziu1iQs737p2cfeo4eWxiuC3OgWFOXjsw7LWZc9y7D/DSOMSpb6/eGN+K/a0fWKrrTXGbqFCRka0vaAGHBxx1zzD2a77W8abH9WLd5hAt4L7NFjeTM/u6bv/WrDPZr9o32Mm+WIU++DnaRZ6RP8veJHt/jDnmcRIdkK879Vc4NSjg1MZ9Kw4fgyhvRFwbiAPpGzY+OtUpvfl4DhSdzjhi/l2She1X1hbaQRAJUcwcnlBspUC5Am7YWxA04gMS9OX3kujacfG5Zc1r7KnGyNdU0RzadWKtxRkLN64nJjhV1M099jll/toGWtTJPZioHsu4Yqfq43UNypZ5fOUz8Dzp+zH+1G0iVmUsjC0pVth/SuoxwZ6ZTRwFevFl2AgsuafkHBR0uqg090Ab2swWl0ojMJvcRAE3JdcJOC8X6Dh51AIux5vOS22sa1GT2nGvg4/YuZ4ua15Clvgc1vXmLtdwiIuyRqkB5v6am4w4y32TvaV9Ah0gQm4kP0yNqxFwRVHLRqtAsEH4UzmKToA5mJTTbPBWfig6id8WViwwWEDRGXVA4NvQT1WIjkEtHlPC9AWcpic6MOl547bPxYBNtPas94A91Z48qvmp5z7IyYx5A5VEJPuiRCD4X9z/8Fbp+6M8C/dd+0DHTqZf+qljJ/RXokgJzkybbKEP2gGfbecxFa0otOLTCrpAu/ce7ctc+VziJgtUsINdn7dmO9/Yx9q3yWdaULTXyX2YXjQsPWFd98HumevnKLydWPfyvNzzMfuFneupM9oO1oRCK77shM/h94d0fVTAmbyO48o+6PZOXGylA3R/j1PjagScTX49AacC0d24nOjjG5UO3HWJ/SB0HMgWc7SHLd4iRvprO75YjYGn5veySX6yX9jXFyY1sXrj2ud667f2xHt69iz3ZwFT7gEBV3wu9AnzRUHU9T99wlXWC/1kXj0f8O2k11kFnJ6D8j3HF9s2bX98llvYJqTOCUawPUFSxntsoXBox1V7DQIOn2dzTGsnO24eu4lF04ZCILJTqMid2NpfC2bvaxJ2X30/h3GcWO/XtHDdzul8GE+8coyl9Qd/rj/R3n0Bp2PA5j+xT2vvHXXAupwHnBoUcLhxD/Cm01XenY07FTrzssXCnhhV21THt2MIXVsemFjosa3MOQs4W/h9YeILCI/pelt8rT3rPdWevs2cAixiDQTFY7B5fvss4zx0TkDj/r8pPm1tIGvcRcDhOquAM8ID2SRWpw1EKdofn23nMRX1mPo7cD1f8PcRCYIt0Cm+4TraMo5r7WjtZAVmpCfGjH8243j3kLuyiV8VE+hTW57AGQFnn+vGhIj16HPtPefBmhNTjDyWr4DEz2C7noBTedL5jO3a3jvqAPPsZl+AU+NqBBwm0Gh4T8ANzlP6vF2zceH38sZT3xrssw9Op2CEdrQBriUV74/5GqwT13YT/gFAKgyNHe3zD0S1xmGfVOBkAZf2VvrVIG4Dt67bjoviphEYuZ+1J35BuVwz+2Lt69nertlS3R/9r37PTwq5tcGbt9sKuPDn2HQvFir7HBk/9imxFa7X51d/qW020eLvfmGbkLgnmLvU/IHFr/rENTQ046IPNX1L/+ofqR32cyTnhD219q33W9FB7spxAQf5JL5U5Xazr76f63FsrMc+apyXVcCVZ9u5nSqdHBLa43pwDY/Ni7O9/81DreEqtjDWu/beVwdk8TySq6bGNAKOPFva4k3uR30Ctx/dRHRAziacSJIsZP69Hk4NCrgrJwXctJxSwB2bFHAkOTOf5F8zktfAqUEBd+WkgJuWFHAkSY7R/sMfe528XE4NCjiSJEmSJMmZOTUo4EiSJEmSJGfm1KCAI0mSJEmSnJlTgwKOJEmSJElyZk4NCjiSJEmSJMmZOTU2FnAEQRAEQRDEaYACjiAIgiAI4sxAAUcQBEEQBHFmoIAjCIIgCII4M1DAEQRBEARBnBko4AiCIAiCIM4MFHAEQRAEQRBnBgo4giAIgiCIM8OeAu55df/qdnV7K1zYDkdCmtck+HIqayIsnj/drW7fLW3zRAg+dGcbiYy7T8+2iSCIufD9fnU31NelbR+wfHfLeLxS7CXgYgEt4i3xUG4Unt1z2nBt8cW27ogLFnB6/+5W999tj9PGpQs4G1unhF7snRRi0Uu2W0JzKHjRpq/uN8hXy9XithZIHTMjuQGe7cVWnEP3+c/N3suzytxH/SHNOfTR+7Qs957F/p0B9B7NmC8o4HaEiaUh5i4Jewq4BSSmlDSWcH1OjAq47377TrhwAVdsOKyzX1BOE5cr4FIsaTzPuNbt0Yu900GwYRVOxbeHQrjIc49iaI1NRbDJehfQP1zrWiEU3F48ibjrXTcoBVrF6HP3JRXzcvCj0u9LLl7x+a2oJLZD2Be1f3PWCgq4HfF80X6+l4DTOA0B572h1rdmCIAcEO0baX17jf0lKHP/ezjdq8/yA+vUoW0Y1l3XUU8PvOKR+zj2PySUgFMnHpJIcS/FH3JhH4pZbDcFvK55mQVc7v8p+UqEepYVWqFo1v5iI+lbTmOMPTEJy+/1pGgxCIf7+BmLdl0bCJWynvo1AowJeQNNz7hPY8R7qq0EYl+5H5+Ney/XfX+oX7NwY8wIovKVjGibfO+GQkch2BfGRjFX4Pi0QhZh970COVxf2jaBeT4i+FX0j7FnC0AI2pznzsnAL+7p5aSxB7Exxl4eMbai5b3cJIIs+EkTO/WkVOe32h+fjXuMJ8REQCvg0t7d51yTal6xd/5sc58+XfVzmvSzz5sTEwg4+B7cAY8nbTILsEnaXldJNQdEQlpDKFBNUjUCbhk/6Lf7c02IGOxoq9CukG2gBMScb5sbApOoTqZLp7BKIOu9q8nPfm9STuB0fy9x28+tb+hkGp4pP8WeZYzBrmU+ZYxlPvkZfop/Dv1gx6r/RR8Pycn3R9lbTzAKVDHoFIr0s7WNijnvFMqKJiNQBPhsO49NYNekfDfDvtDZBNyzT0TMBzJf70WhQp12yb4ZOzQFO0DlnAwp4PCcEsfW1ivrm/nPSU4/YhvofG9FUyOovNyk9rbWH/GLghwfjS+A/8jzyhgZ28bMZcL8CXWwSbsngKwRmj7ysjaS0xIOqwX2FHD+m/Qh0BSLFZ5ImDlB0ittSsDVBN+soyvgdGL1CsSpA22IxawRcN+raBD7iAg5JpSAM3ufAgheLkpbT3SIYBP0BZz1OxvQrYDT9iz2BXuiMJb5pCur5HuhHyYP9OnMNEZaM84RC0xgwC4CDteefmrhouIrANbXb6uxhH6Hz7bz2AR2n2xxW+u/Ever1j5xPjaJj6DOJa01wgg4D3YNYR5oh3GbpL1xEXPZ+eWr00ErlmJr9t9GwJkYibnBCLJSPzrxYftjLkABp561xr+uA/ginFucfGJzWNtnmWzf7E9uM3tsfWMu7CXgYkK5dd5ODwCb3AKsqIrARInG7wk44/SlqDQCri3S1lFOHWhDm3QUSjEL6ww2gEJ0RGCQWV9ok6wEck+Qpf4VqZ/X3waw/dz6hn8Ch/ZcykURa6v6HadlmGPwS/R5EBgK0ccXZQ52vl6RsQJFFR+4F19SUJDUHgaeSLFtUIjQ7/DZdh6bwN5jT13HE6xTDCGxt742DrFvnJMds2u/Nqc0f2Ho3rtmX1bo98QusP4lbQE2l7p2HhNw5TQn9wufrYCDWlYF3PnVoPmxgYD7An/NyHZt+kiOtvlL2rbMT1NhLwHnJbml7TQTvMCwb6gBuBHqekfAqS/9xkSYi60TcEoY2k09A2gbglAZ1qqEDwiQcM/inZwSHRe4t+2Xso3IHtY0LuDS77KqVGxbAdeeXojQqoLRFtow1lI+GL8Te1bUceR7M4svWVCYhOKJECvctDire7mxgJO1xLfM6v94r425usYUP9L3Pv7UbbbYCXBv22S6CfS+lft3SLY4R3va2sPyHdiq8c3V+pwBwlZg96l3f0+gLj+JMEh74PUhNgXUhgzxX7VPMRac/XfqSdoPPW4ZK+cdGRf3T/rEOOn4xPVivYDDvCP5zPYJ+5I+Yf5aqpx2DOwl4Jo/n9weVsDZZ0uBks9p42pbKJTrBJwEShpj0fwjhmW5I2926HemQYPiRT5LkcYv6atE30tIR4AKsigw0BdM2yv5l3l9ARcg/Zt/xIDrVT6Cp2m+gAuoczMnI9meCnn8dcA5JB98hrWIWEvJJfULYrEtMlYYoKCqX/bVa7LPkfFjHxVb9fn2xSjZWfsf/r6fgFup/S+zbfJGu1cWaB99b18EldxwK3nIYI2A89es85t/r3N6CCec6+ZNbAdr5wAbT25u6gq4+Kn01UIw/SM61R56m1jmHiPWC7jQp9gsa4S2zxLytpPTvst3aGu8HQJ7CTji+tB7uyd2wynbs01iBEEQxKmAAo7YDPIWyYI+Dc7AnhRwBEEQp4v/B1epzKFY4yLIAAAAAElFTkSuQmCC>