import os
import sys


def usage() -> None:
    print("usage: python config_parameter.py <dax-device> [fb_mode_on]")


def configure(dax_device: str, filebench: bool) -> None:
    with open("./config/config_template.h", "r", encoding="utf-8") as source:
        lines = source.readlines()

    configured = []
    for line in lines:
        if "ORCH_DEV_NVM_PATH" in line:
            configured.append(
                f'#define ORCH_DEV_NVM_PATH     "{dax_device}"\n'
            )
        elif filebench and "// #define FILEBENCH" in line:
            configured.append("#define FILEBENCH\n")
        else:
            configured.append(line)

    with open("./config/config.h", "w", encoding="utf-8") as output:
        output.writelines(configured)


def main(arguments: list[str]) -> int:
    if len(arguments) not in (2, 3) or (
        len(arguments) == 3 and arguments[2] != "fb_mode_on"
    ):
        usage()
        return 2
    dax_device = arguments[1]
    if not os.path.exists(dax_device):
        print(f"DAX device does not exist: {dax_device}")
        return 1
    configure(dax_device, len(arguments) == 3)
    print(f"Byte-addressed device: {dax_device}")
    print(f"Filebench mode: {'on' if len(arguments) == 3 else 'off'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
