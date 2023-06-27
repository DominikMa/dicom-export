import subprocess
from pathlib import Path
from typing import Optional, Literal, List, Union, Tuple

from pydantic import BaseModel, conint, Field


class Args(BaseModel):

    @classmethod
    def _to_args(cls, values, fields, fields_set):
        args = []
        for field_name in fields_set:
            if fields[field_name].type_ == bool and values[field_name]:
                args.append(f'--{fields[field_name].alias}')
            elif fields[field_name].type_ != bool:
                args.append(f'--{fields[field_name].alias}')
                args.append(str(values[field_name]))
        return args

    def to_args(self):
        return self._to_args(
            values=self.dict(),
            fields=self.__fields__,
            fields_set=self.__fields_set__
        )


class Parameters(Args):
    peer: str
    port: int

    @classmethod
    def _to_args(cls, values: dict, fields: dict, fields_set: set):
        positional_args = []
        for field_name in ['peer', 'port']:
            positional_args.append(str(values[field_name]))
            fields_set.remove(field_name)
            del fields[field_name]
            del values[field_name]

        args = super(Parameters, cls)._to_args(values, fields, fields_set)
        return args + positional_args


class GeneralOptions(Args):
    help: bool = False
    version: bool = False
    arguments: bool = False
    quiet: bool = False
    verbose: bool = False
    debug: bool = False
    log_level: Optional[Literal['fatal', 'error', 'warn', 'info', 'debug', 'trace']] = Field(alias='log-level')
    log_config: Optional[Path] = Field(alias='log-config')


class NetworkOptionsBase(Args):
    aetitle: str
    call: str


class NetworkOptionsEcho(NetworkOptionsBase):
    # application entity titles
    aetitle: str = 'ECHOSCU'


class NetworkOptionsFind(NetworkOptionsBase):
    # override matching keys
    # TODO
    keys: Optional[List[Tuple[Union[Tuple[str, str], str], str]]] = None

    # query information model
    worklist: bool = False
    patient: bool = False
    study: bool = False
    psonly: bool = False

    # application entity titles
    aetitle: str = 'FINDSCU'

    @classmethod
    def _to_args(cls, values: dict, fields: dict, fields_set: set):
        keys = []
        for field_name in ['peer', 'port']:
            positional_args.append(str(values[field_name]))
            fields_set.remove(field_name)
            del fields[field_name]
            del values[field_name]

        args = super(Parameters, cls)._to_args(values, fields, fields_set)
        return args + positional_args


class NetworkOptionsMove(NetworkOptionsBase):
    # override matching keys
    # TODO
    keys: Optional[List] = None

    # query information model
    patient: bool = False
    study: bool = False
    psonly: bool = False

    # application entity titles
    aetitle: str = 'MOVESCU'
    move: str = 'MOVESCU'


class EchoOptions(Parameters, GeneralOptions, NetworkOptionsEcho):
    pass


class FindOptions(Parameters, GeneralOptions, NetworkOptionsFind):
    pass


class MoveOptions(Parameters, GeneralOptions, NetworkOptionsMove):
    pass


class Base:
    def __init__(
            self,
            binary_path: Path,
            args: Args
    ):
        self.binary_path = binary_path
        self.args = args

    def run(self):
        command = [self.binary_path] + self.args.to_args()
        raw_response = subprocess.run(
            command,
            capture_output=True,
            shell=False
        )
        print(raw_response.stdout.decode('utf-8'))
        return raw_response


class Echo(Base):
    def __init__(
            self,
            args: EchoOptions,
            binary_path: Path = 'echoscu',
    ):
        super().__init__(binary_path=binary_path, args=args)


class Find(Base):
    def __init__(
            self,
            args: FindOptions,
            binary_path: Path = 'findscu',
    ):
        super().__init__(binary_path=binary_path, args=args)


class Move(Base):
    def __init__(
            self,
            args: MoveOptions,
            binary_path: Path = 'movescu',
    ):
        super().__init__(binary_path=binary_path, args=args)


if __name__ == '__main__':
    Echo(args=EchoOptions(peer="www.dicomserver.co.uk", port=11112, aetitle="TEST1234", call="TEST1234")).run()

    find_options = FindOptions(peer="www.dicomserver.co.uk", port=11112, aetitle="TEST1234", call="TEST1234")
    b = Find(args=find_options)
    b.run()
    print()
