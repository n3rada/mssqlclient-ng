Enhanced version of impacket's `mssqlclient.py`. It lets you interact with Microsoft SQL Server (MS SQL / MSSQL) servers and their linked instances, impersonating any account encountered along the way, without requiring complex T-SQL queries.


<p align="center">
    <img src="./media/example.png" alt="example">
</p>

N.B. It can handle NTLM relaying 🔄


> [!TIP]
> If you have only access to a MS SQL instance through your implant/beacon, use [MSSQLand](https://github.com/n3rada/MSSQLand), the `C#` version built with assembly execution in mind.


## 📦 Installation

To install `mssqlclient-ng`, you can use `pip`, `pip3` or `pipx`. Either from `pypi` repository or from `GitHub` source. Prefer using [`pipx`](https://pypa.github.io/pipx/), since it install Python applications in isolated virtual environments.

### From [PyPI](https://pypi.org/project/mssqlclient-ng/)

```bash
pipx install mssqlclient-ng
```

```bash
pip install mssqlclient-ng
```

### From GitHub

```bash
pipx install 'git+https://github.com/n3rada/mssqlclient-ng.git'
```


## 🧸 Usage

```shell
mssqlclient-ng <host> [options]
```

> [!TIP]
> Avoid typing out all the **[RPC Out](https://learn.microsoft.com/fr-fr/sql/t-sql/functions/openquery-transact-sql)** or **[OPENQUERY](https://learn.microsoft.com/fr-fr/sql/t-sql/functions/openquery-transact-sql)** calls manually. Let the tool handle any linked servers chain with the `-l` argument, so you can focus on the big picture.


Format: `server,port:user@database` or any combination `server:user@database,port`.
- `server` (required) - The SQL Server hostname or IP
- `,port` (optional) - Port number (default: 1433, also common: 1434, 14333, 2433)
- `:user` (optional) - User to impersonate on this server
- `@database` (optional) - Database context (defaults to 'master' if not specified)

```shell
mssqlclient-ng localhost -c token
```

> [!IMPORTANT]
> The **host** (first argument) and **action** (after flags) are positional arguments. All flags use `-` prefix. For example: `localhost -c token createuser -p p@ssword!` - here `-p` belongs to the action, not the global arguments.

**Common options:**
- `--timeout 30` - Connection timeout in seconds (default: 15)
- `-l SERVER1:user1,SERVER2:user2@dbclients` - Chain through linked servers (uses configured linked server names)

> [!NOTE]
> Port specification (`,port`) only applies to the initial host connection. Linked server chains (`-l`) use the linked server names as configured in `sys.servers`, not `hostname:port` combinations.

## 🤝 Contributing 

Contributions are welcome and appreciated! Whether it's fixing bugs, adding new features, improving the documentation, or sharing feedback, your effort is valued and makes a difference.
Open-source thrives on collaboration and recognition. Contributions, large or small, help improve the tool and its community. Your time and effort are truly valued. 

Here, no one will be erased from Git history. No fear to have here. No one will copy-paste your code without adhering to the collaborative ethos of open-source.

## 🙏 Acknowledgments

- Built upon [Impacket](https://github.com/fortra/impacket), based on the core [tds.py](https://github.com/fortra/impacket/blob/master/impacket/tds.py).
- OOP design is really tied to [MSSQLand](https://github.com/n3rada/MSSQLand).
- Terminal interface powered by [prompt_toolkit](https://github.com/prompt-toolkit/python-prompt-toolkit).

## ⚠️ Disclaimer

**This tool is provided strictly for defensive security research, education, and authorized penetration testing.** You must have **explicit written authorization** before running this software against any system you do not own.

This tool is designed for educational purposes only and is intended to assist security professionals in understanding and testing the security of SQL Server environments in authorized engagements.

Acceptable environments include:
- Private lab environments you control (local VMs, isolated networks).  
- Sanctioned learning platforms (CTFs, Hack The Box, OffSec exam scenarios).  
- Formal penetration-test or red-team engagements with documented customer consent.

Misuse of this project may result in legal action.
