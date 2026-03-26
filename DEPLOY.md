# Деплой на Linux (Ubuntu 22.04)

## Часть 0 — заказать сервер на хостинге

Переходим на хостинг, заказываем себе сервер, я буду использовать сервис zomro.

https://zomro.com/vps?from=373283


ssh root@<IP-сервера>
---

## Часть 1 — Установка Docker на чистую Ubuntu 22.04 (один раз выполнить команды)

Все команды выполняются от имени пользователя с правами sudo.

### Шаг 1 — Обновить систему

```bash
sudo apt update && sudo apt upgrade -y
```

### Шаг 2 — Установить зависимости

```bash
sudo apt install -y ca-certificates curl gnupg lsb-release
```

### Шаг 3 — Добавить официальный GPG-ключ Docker

```bash
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
```

### Шаг 4 — Добавить репозиторий Docker

```bash
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
```

### Шаг 5 — Установить Docker Engine

```bash
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

### Шаг 6 — Проверить установку

```bash
sudo docker run hello-world
```

Должна появиться строка: `Hello from Docker!`

---

## Часть 2 — Деплой приложения

### Шаг 1 — Загрузить проект на сервер и перейти в папку с ним

```bash
git clone https://github.com/eaxgtfcs-glitch/funding;cd funding
```

### Шаг 2 — Создать файл .env

```bash
nano .env

```

бот в телеге создается через @BotFather
chat_id(id) можно узнать через бота @userinfobot — просто напиши ему.

скопируй свой .env файл с ключами и конфигурациями

Сохрани файл: `Ctrl+O`, затем `Ctrl+X`.

### Шаг 3 — Запустить контейнер с обновлением из репозитория и сразу выводом логов

```bash(Linux)
 cd funding;git pull https://github.com/eaxgtfcs-glitch/funding;docker stop funding;docker rm funding;docker build -t funding-app .;docker run -d   --name funding   --restart unless-stopped   --env-file .env   funding-app;docker logs -f funding
```

```powerShell (Windows)
 git pull https://github.com/eaxgtfcs-glitch/funding;docker stop funding; docker rm funding; docker build -t funding-app .; docker run -d --name funding --restart unless-stopped --env-file .env funding-app; docker logs -f funding
```
Должны появиться логи вида:

```
2026-03-18 16:39:47,171 CRITICAL __main__ — Starting application
```

## Управление контейнером

| Действие                             | Команда                  |
|--------------------------------------|--------------------------|
| Посмотреть логи                      | `docker logs funding`    |
| Следить за логами в реальном времени | `docker logs -f funding` |
| Остановить                           | `docker stop funding`    |
| Запустить снова                      | `docker start funding`   |
| Перезапустить                        | `docker restart funding` |
| Удалить контейнер                    | `docker rm -f funding`   |
