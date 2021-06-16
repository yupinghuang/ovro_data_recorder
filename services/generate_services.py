#!/usr/bin/env python

import os
import sys
import glob
import jinja2
import argparse

# Setup
## Power beam setup
rdir = '/home/ubuntu/data/beam'
quota = 0
beams = { 1: ('enp216s0', 10000, rdir+'01', quota),
          2: ('enp216s0', 10001, rdir+'02', quota),
          3: ('enp216s0', 10002, rdir+'03', quota),
          4: ('enp216s0', 10003, rdir+'04', quota),
          5: ('enp216s0', 10004, rdir+'05', quota),
          6: ('enp216s0', 10005, rdir+'06', quota),
          7: ('enp216s0', 10006, rdir+'07', quota),
          8: ('enp216s0', 10007, rdir+'08', quota),
          9: ('enp216s0', 10008, rdir+'09', quota),
         10: ('enp216s0', 10009, rdir+'10', quota),
         11: ('enp216s0', 10010, rdir+'11', quota),
         12: ('enp216s0', 10011, rdir+'12', quota),
        }

## Slow visibilities setup
rdir = '/home/ubuntu/data/slow'
quota = 0
vslow = { 1: ('10.41.0.17', 10000, rdir, quota),
          2: ('10.41.0.18', 10000, rdir, quota),
          3: ('10.41.0.19', 10000, rdir, quota),
          4: ('10.41.0.20', 10000, rdir, quota),
          5: ('10.41.0.21', 10000, rdir, quota),
          6: ('10.41.0.22', 10000, rdir, quota),
          7: ('10.41.0.23', 10000, rdir, quota),
          8: ('10.41.0.24', 10000, rdir, quota),
          9: ('10.41.0.33', 10000, rdir, quota),
         10: ('10.41.0.34', 10000, rdir, quota),
         11: ('10.41.0.35', 10000, rdir, quota),
         12: ('10.41.0.36', 10000, rdir, quota),
         13: ('10.41.0.37', 10000, rdir, quota),
         14: ('10.41.0.38', 10000, rdir, quota),
         15: ('10.41.0.39', 10000, rdir, quota),
         16: ('10.41.0.40', 10000, rdir, quota),
        }

## Fast visibilities setup
rdir = '/home/ubuntu/data/fast'
quota = 10*1024**4
vfast = { 1: ('10.41.0.17', 11000, rdir, quota),
          2: ('10.41.0.18', 11000, rdir, quota),
          3: ('10.41.0.19', 11000, rdir, quota),
          4: ('10.41.0.20', 11000, rdir, quota),
          5: ('10.41.0.21', 11000, rdir, quota),
          6: ('10.41.0.22', 11000, rdir, quota),
          7: ('10.41.0.23', 11000, rdir, quota),
          8: ('10.41.0.24', 11000, rdir, quota),
          9: ('10.41.0.33', 11000, rdir, quota),
         10: ('10.41.0.34', 11000, rdir, quota),
         11: ('10.41.0.35', 11000, rdir, quota),
         12: ('10.41.0.36', 11000, rdir, quota),
         13: ('10.41.0.37', 11000, rdir, quota),
         14: ('10.41.0.38', 11000, rdir, quota),
         15: ('10.41.0.39', 11000, rdir, quota),
         16: ('10.41.0.40', 11000, rdir, quota),
        }

def main(args):
    # Pre-process
    if (not args.power_beams \
        and not args.slow_visibilities \
        and not args.fast_visibilities):
       args.power_beams = args.slow_visibilities = args.fast_visibilities = True
       
    # Render
    loader = jinja2.FileSystemLoader(searchpath='./')
    env = jinja2.Environment(loader=loader)

    ## Power beams
    if args.power_beams:
        if args.clean:
            filenames = glob.glob('./dr-beam-[0-9]*.service')
            for filename in filenames:
                os.unlink(filename)
        else:
            template = env.get_template('dr-beam-base.service')
            for beam in beams:
                address, port, directory, quota = beams[beam] 
                service = template.render(beam=beam, address=address, port=port,
                                          directory=directory, quota=quota)
                with open('dr-beam-%s.service' % beam, 'w') as fh:
                    fh.write(service)

    ## Slow visibilities
    if args.slow_visibilities:
        if args.clean:
            filenames = glob.glob('./dr-vslow-[0-9]*.service')
            filenames.extend(glob.glob('./dr-manager-vslow.service'))
            for filename in filenames:
                os.unlink(filename)
        else:
            ### Recorders
            template = env.get_template('dr-vslow-base.service')
            for band in vslow:
                address, port, directory, quota = vslow[band]
                service = template.render(band=band, address=address, port=port,
                                          directory=directory, quota=quota)
                with open('dr-vslow-%s.service' % band, 'w') as fh:
                    fh.write(service)

            ### Manager
            template = env.get_template('dr-manager-vslow-base.service')
            begin_band = min([band for band in vslow])
            end_band = max([band for band in vslow])
            service = template.render(begin_band=begin_band, end_band=end_band)
            with open('dr-manager-vslow.service', 'w') as fh:
                fh.write(service)

    ## Fast visibilities
    if args.fast_visibilities:
        if args.clean:
            filenames = glob.glob('./dr-vfast-[0-9]*.service')
            filenames.extend(glob.glob('./dr-manager-vfast.service'))
            for filename in filenames:
                os.unlink(filename)
        else:
            ### Recorders
            emplate = env.get_template('dr-vfast-base.service')
            for band in vfast:
                address, port, directory, quota = vfast[band]
                service = template.render(band=band, address=address, port=port,
                                          directory=directory, quota=quota)
                with open('dr-vfast-%s.service' % band, 'w') as fh:
                    fh.write(service)
                    
            ### Manager
            template = env.get_template('dr-manager-vfast-base.service')
            begin_band = min([band for band in vfast])
            end_band = max([band for band in vfast])
            service = template.render(begin_band=begin_band, end_band=end_band)
            with open('dr-manager-vfast.service', 'w') as fh:
                fh.write(service)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='generate systemd service files for the data recorder pipelines', 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-b', '--power-beams', action='store_true',
                       help='only generate/clean the power beam services')
    group.add_argument('-s', '--slow-visibilities', action='store_true',
                       help='only generate/clean the slow visibitlies services')
    group.add_argument('-f', '--fast-visibilities', action='store_true',
                       help='only generate/clean the fast visibilities services')
    group.add_argument('-a', '--all', action='store_false',
                       help='generate/clean all services')
    parser.add_argument('-c', '--clean', action='store_true',
                        help='delete the generated services')
    args = parser.parse_args()
    main(args)
    