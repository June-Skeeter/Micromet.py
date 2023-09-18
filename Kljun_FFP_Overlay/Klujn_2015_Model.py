
# Derive a flux footprint estimate based on the simple parameterisation FFP
# See Kljun, N., P. Calanca, M.W. Rotach, H.P. Schmid, 2015: 
# The simple two-dimensional parameterisation for Flux Footprint Predictions FFP.
# Geosci. Model Dev. 8, 3695-3713, doi:10.5194/gmd-8-3695-2015, for details.
# See original code here: 

# Adapted for multi-processing by June Skeeter 5/26/2023
# Removed option to calculate from umean instead of ustar
# Added option to intersect with basemap: https://footprint.kljun.net/

import numpy as np

def FFP(index,ustar,sigmav,h,ol,wind_dir,z0,zm,theta,rho,x_2d,basemap=None):
    
    #===========================================================================
    # Model parameters
    a = 1.4524
    b = -1.9914
    c = 1.4622
    d = 0.1359
    ac = 2.17 
    bc = 1.66
    cc = 20.0

    xstar_end = 30
    oln = 5000 #limit to L for neutral scaling
    k = 0.4 #von Karman

    if wind_dir is not None:
        rotated_theta = theta - wind_dir * np.pi / 180.

    #===========================================================================
    # Create real scale crosswind integrated footprint and dummy for
    # rotated scaled footprint
    fstar_ci_dummy = np.zeros(x_2d.shape)
    f_ci_dummy = np.zeros(x_2d.shape)

    if ol <= 0 or ol >= oln:
        xx = (1 - 19.0 * zm/ol)**0.25
        psi_f = (np.log((1 + xx**2) / 2.) + 2. * np.log((1 + xx) / 2.) - 2. * np.arctan(xx) + np.pi/2)
    elif ol > 0 and ol < oln:
        psi_f = -5.3 * zm / ol
    else:
        print('OL_Flag')
        print(ol)
    if (np.log(zm / z0)-psi_f)>0:
        xstar_ci_dummy = (rho * np.cos(rotated_theta) / zm * (1. - (zm / h)) / (np.log(zm / z0) - psi_f))
        px = np.where(xstar_ci_dummy > d)
        fstar_ci_dummy[px] = a * (xstar_ci_dummy[px] - d)**b * np.exp(-c / (xstar_ci_dummy[px] - d))
        f_ci_dummy[px] = (fstar_ci_dummy[px] / zm * (1. - (zm / h)) / (np.log(zm / z0) - psi_f))
    else:
        flag_err = 1
    #===========================================================================
    # Calculate dummy for scaled sig_y* and real scale sig_y
    sigystar_dummy = np.zeros(x_2d.shape)
    sigystar_dummy[px] = (ac * np.sqrt(bc * np.abs(xstar_ci_dummy[px])**2 / (1 +
                            cc * np.abs(xstar_ci_dummy[px]))))

    if abs(ol) > oln:
        ol = -1E6
    if ol <= 0:   #convective
        scale_const = 1E-5 * abs(zm / ol)**(-1) + 0.80
    elif ol > 0:  #stable
        scale_const = 1E-5 * abs(zm / ol)**(-1) + 0.55
    if scale_const > 1:
        scale_const = 1.0

    sigy_dummy = np.zeros(x_2d.shape)
    sigy_dummy[px] = (sigystar_dummy[px] / scale_const * zm * sigmav / ustar)
    sigy_dummy[sigy_dummy < 0] = np.nan

    #===========================================================================
    # Calculate real scale f(x,y)
    f_2d = np.zeros(x_2d.shape)
    f_2d[px] = (f_ci_dummy[px] / (np.sqrt(2 * np.pi) * sigy_dummy[px]) *
                np.exp(-(rho[px] * np.sin(rotated_theta[px]))**2 / ( 2. * sigy_dummy[px]**2)))
    
    
    #===========================================================================
    # Normalize f_2d to force values to sum to 1
    f_2d = f_2d/f_2d.sum()

    if basemap is None:
        return(index,f_2d)
    else:
        class_sums = np.empty((0), float)
        for v in range(1,int(np.nanmax(basemap)+1)):
            temp = basemap.copy()
            temp[basemap!=v]=0
            temp[basemap==v]=1
            class_sums = np.append(class_sums,np.nansum(temp*f_2d))
        return(index,f_2d,class_sums)

