# Global Memory and Threading (GMT)

## Build Instructions

### List of Dependencies

The only dependencies for GMT are:
- Linux
- A C compiler
- MPI

### How to build GMT

Before attempting to build GMT, please take a look at the requirements in [List of Dependencies](#list-of-dependencies).  Building GMT is as simple as:
```
git clone https://github.com/pnnl/gmt.git
cd gmt
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$GMT_ROOT \
         -DCMAKE_BUILD_TYPE=Release
make -j <SOMETHING_REASONABLE> && make install
```
where $GMT_ROOT is the desidered installation prefix.

## Acknowledgments

This material was prepared as an account of work sponsored by an agency of the United States Government.  Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any of their employees, nor any jurisdiction or organization that has cooperated in the development of these materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process disclosed, or represents that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer, or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed herein do not necessarily state or reflect those of the United States Government or any agency thereof.

                     PACIFIC NORTHWEST NATIONAL LABORATORY
                                  operated by
                                    BATTELLE
                                    for the
                       UNITED STATES DEPARTMENT OF ENERGY
                        under Contract DE-AC05-76RL01830

