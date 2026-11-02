import java.io._
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import java.util.Scanner

// Простая модель записи
case class VaultRecord(
                        id: Int,
                        service: String,
                        username: String,
                        password: String,
                        email: String,
                        url: String,
                        note: String,
                        createdAt: Long,
                        updatedAt: Long
                      )

object SimpleVault {

  // Настройки и состояние
  var config: Map[String, String] = Map.empty
  var records: List[VaultRecord] = Nil
  var nextId: Int = 1
  var isUnlocked: Boolean = false
  var lastActivityTime: Long = System.currentTimeMillis()
  var failedAttempts: Int = 0

  val STORAGE_FILE = "vault.dat"
  val LOG_FILE = "vault.log"
  val CONFIG_FILE = "config.txt"

  // Ключ шифрования
  var encryptionKey: Array[Byte] = null

  def main(args: Array[String]): Unit = {
    loadConfig()
    log("INFO", "Приложение запущено")

    println("Локальное хранилище паролей. Команда 'help' для списка команд.")

    // Запуск потока автоблокировки
    startAutoLockThread()

    val scanner = new java.util.Scanner(System.in)

    var running = true

    while (running) {
      print(if (isUnlocked) "[OPEN]> " else "> ")

      if (!scanner.hasNextLine()) {
        running = false
      } else {
        val line = scanner.nextLine().trim

        if (line.nonEmpty) {
          processCommand(line, scanner)

          if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
            log("INFO", "Получена команда завершения")
            running = false
          }
        }
      }
    }

    scanner.close()
    log("INFO", "Приложение завершено")
  }

  
  // Конфигурация
  def loadConfig(): Unit = {
    val file = new File(CONFIG_FILE)
    if (!file.exists()) {
      // Дефолтный конфиг
      Files.write(file.toPath,
        s"""storageFile=$STORAGE_FILE\nlogFile=$LOG_FILE\nautomaticLockMinutes=5\nmaximumUnlockAttempts=3\ndefaultSort=service\nconfirmBeforeDelete=true""".getBytes
      )
      log("WARN", "Конфигурация создана по умолчанию")
    }

    val lines = Files.readAllLines(file.toPath()).toArray.map(_.toString)
    config = lines.flatMap { l =>
      val parts = l.split("=", 2)
      if (parts.length == 2) Some(parts(0).trim -> parts(1).trim) else None
    }.toMap

    validateConfig()
  }

  def validateConfig(): Unit = {
    val errors = ListBuffer[String]()

    if (!config.contains("storageFile")) errors += "Не указан storageFile"
    if (!config.contains("logFile")) errors += "Не указан logFile"
    if (config.get("storageFile").contains(config.get("logFile"))) errors += "storageFile и logFile совпадают"

    Try(config("automaticLockMinutes").toInt).foreach { v =>
      if (v < 1) errors += "automaticLockMinutes должен быть >= 1"
    }

    Try(config("maximumUnlockAttempts").toInt).foreach { v =>
      if (v < 1) errors += "maximumUnlockAttempts должен быть >= 1"
    }

    val validSorts = Set("id", "service", "username", "created", "updated")
    if (!validSorts.contains(config.getOrElse("defaultSort", ""))) errors += "Неверный defaultSort"

    if (errors.nonEmpty) {
      println("ОШИБКА КОНФИГУРАЦИИ:")
      errors.foreach(e => println(s"- $e"))
      sys.exit(1)
    }
    log("INFO", "Конфигурация загружена успешно")
  }

  // Логирование
  def log(level: String, msg: String): Unit = {
    try {
      val ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      val writer = new BufferedWriter(new FileWriter(LOG_FILE, true))
      writer.write(s"$ts $level  $msg")
      writer.newLine()
      writer.close()
    } catch { case _: Exception => () }
  }

  // Автоблокировка
  def startAutoLockThread(): Unit = {
    val minutes = config.getOrElse("automaticLockMinutes", "5").toInt
    val millis = minutes * 60 * 1000L

    val thread = new Thread(() => {
      while (true) {
        Thread.sleep(1000)
        if (isUnlocked && System.currentTimeMillis() - lastActivityTime > millis) {
          lockVault(auto = true)
        }
      }
    })
    thread.setDaemon(true)
    thread.start()
  }

  def touchActivity(): Unit = {
    lastActivityTime = System.currentTimeMillis()
  }

  def deriveKey(password: String): Array[Byte] = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.digest(password.getBytes("UTF-8"))
  }

  def encrypt(data: Array[Byte], key: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val iv = new Array[Byte](16) // Initialization Vector
    new SecureRandom().nextBytes(iv)
    cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new javax.crypto.spec.IvParameterSpec(iv))
    val encrypted = cipher.doFinal(data)
    iv ++ encrypted
  }

  def decrypt(data: Array[Byte], key: Array[Byte]): Array[Byte] = {
    val iv = data.take(16)
    val encrypted = data.drop(16)
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new javax.crypto.spec.IvParameterSpec(iv))
    cipher.doFinal(encrypted)
  }

  // Работа с файлом хранилища
  def saveVault(): Unit = {
    if (!isUnlocked || encryptionKey == null) return

    try {
      val baos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(baos)

      // Количество записей
      dos.writeInt(records.size)

      records.foreach { r =>
        dos.writeInt(r.id)
        dos.writeUTF(r.service)
        dos.writeUTF(r.username)
        dos.writeUTF(r.password)
        dos.writeUTF(r.email)
        dos.writeUTF(r.url)
        dos.writeUTF(r.note)
        dos.writeLong(r.createdAt)
        dos.writeLong(r.updatedAt)
      }

      val plainData = baos.toByteArray
      val encryptedData = encrypt(plainData, encryptionKey)

      Files.write(Paths.get(STORAGE_FILE), encryptedData)
      log("INFO", "Хранилище сохранено")
    } catch {
      case e: Exception =>
        log("ERROR", s"Ошибка сохранения: ${e.getMessage}")
        println("Ошибка сохранения данных!")
    }
  }

  def loadVault(password: String): Boolean = {
    val file = new File(STORAGE_FILE)
    if (!file.exists()) return false

    try {
      val encryptedData = Files.readAllBytes(file.toPath())
      val key = deriveKey(password)

      val plainData = decrypt(encryptedData, key)

      val bais = new ByteArrayInputStream(plainData)
      val dis = new DataInputStream(bais)

      val count = dis.readInt()
      val loadedRecords = ListBuffer[VaultRecord]()

      for (_ <- 1 to count) {
        val id = dis.readInt()
        val service = dis.readUTF()
        val username = dis.readUTF()
        val pass = dis.readUTF()
        val email = dis.readUTF()
        val url = dis.readUTF()
        val note = dis.readUTF()
        val created = dis.readLong()
        val updated = dis.readLong()

        loadedRecords += VaultRecord(id, service, username, pass, email, url, note, created, updated)
      }

      this.records = loadedRecords.toList
      this.nextId = if (records.nonEmpty) records.maxBy(_.id).id + 1 else 1
      this.encryptionKey = key
      true
    } catch {
      case _: Exception => false
    }
  }

  // Команды
  def processCommand(cmdLine: String, scanner: Scanner): Unit = {
    val parts = cmdLine.split("\\s+", 2)
    val cmd = parts(0).toLowerCase
    val arg = if (parts.length > 1) parts(1) else ""

    cmd match {
      case "help" => printHelp()
      case "exit" | "quit" =>
      case "create" => handleCreate(scanner)
      case "open" => handleOpen(scanner)
      case "lock" => lockVault(auto = false)
      case "list" => if (checkAccess()) listRecords()
      case "show" => if (checkAccess()) showRecord(arg.toIntOption.getOrElse(-1))
      case "pass" => if (checkAccess()) showPassword(arg.toIntOption.getOrElse(-1))
      case "add" => if (checkAccess()) addRecord(scanner)
      case "edit" => if (checkAccess()) editRecord(arg.toIntOption.getOrElse(-1), scanner)
      case "remove" => if (checkAccess()) removeRecord(arg.toIntOption.getOrElse(-1), scanner)
      case "search" => if (checkAccess()) searchRecords(arg)
      case _ => println("Неизвестная команда. Введите help.")
    }

    if (Set("list", "show", "pass", "add", "edit", "remove", "search").contains(cmd)) {
      touchActivity()
    }
  }

  def checkAccess(): Boolean = {
    if (!isUnlocked) {
      println("Хранилище закрыто. Используйте команду open.")
      false
    } else true
  }

  def printHelp(): Unit = {
    println("""
              |Команды:
              | create       - Создать новое хранилище (стирает старое)
              | open         - Открыть существующее хранилище
              | lock         - Заблокировать хранилище
              | list         - Список всех записей
              | show <id>    - Показать детали записи
              | pass <id>    - Показать пароль записи
              | add          - Добавить новую запись
              | edit <id>    - Изменить запись
              | remove <id>  - Удалить запись
              | search <txt> - Поиск по сервису/логину/url
              | exit         - Выход
    """.stripMargin)
  }

  def handleCreate(scanner: Scanner): Unit = {
    if (new File(STORAGE_FILE).exists()) {
      print("Файл уже существует. Перезаписать? [y/n]: ")
      if (!scanner.nextLine().equalsIgnoreCase("y")) {
        println("Отменено.")
        return
      }
    }

    print("Мастер-пароль: ")
    val p1 = readSecret(scanner)
    print("Повторите пароль: ")
    val p2 = readSecret(scanner)

    if (p1 != p2 || p1.isEmpty) {
      println("Пароли не совпадают или пусты.")
      return
    }

    this.records = Nil
    this.nextId = 1
    this.encryptionKey = deriveKey(p1)
    this.isUnlocked = true
    saveVault()
    log("INFO", "Хранилище создано")
    println("Хранилище создано и открыто.")
  }

  def handleOpen(scanner: Scanner): Unit = {
    if (isUnlocked) {
      println("Уже открыто.")
      return
    }

    val maxAttempts = config.getOrElse("maximumUnlockAttempts", "3").toInt

    if (failedAttempts >= maxAttempts) {
      println("Превышено число попыток. Перезапустите программу.")
      return
    }

    print("Мастер-пароль: ")
    val pwd = readSecret(scanner)

    if (loadVault(pwd)) {
      isUnlocked = true
      failedAttempts = 0
      log("INFO", "Хранилище открыто")
      println("Доступ разрешен.")
    } else {
      failedAttempts += 1
      log("WARN", s"Неудачная попытка открытия ($failedAttempts/$maxAttempts)")
      println(s"Неверный пароль. Осталось попыток: ${maxAttempts - failedAttempts}")
    }
  }

  def lockVault(auto: Boolean): Unit = {
    if (!isUnlocked) return
    isUnlocked = false
    encryptionKey = null
    records = Nil
    if (auto) log("INFO", "Автоблокировка выполнена")
    else log("INFO", "Ручная блокировка выполнена")
    println("Хранилище заблокировано.")
  }

  def listRecords(): Unit = {
    if (records.isEmpty) {
      println("Записей нет.")
      return
    }
    println(f"${"ID"}%-5s ${"Сервис"}%-20s ${"Логин"}%-20s ${"Изменена"}%-20s")
    val sorted = sortRecords(records)
    sorted.foreach { r =>
      val dateStr = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(r.updatedAt), java.time.ZoneId.systemDefault()).toString.replace('T', ' ').take(19)
      println(f"${r.id}%-5d ${r.service.take(20)}%-20s ${r.username.take(20)}%-20s $dateStr%-20s")
    }
  }

  def sortRecords(recs: List[VaultRecord]): List[VaultRecord] = {

    recs.sortBy(_.id)
  }

  def showRecord(id: Int): Unit = {
    records.find(_.id == id) match {
      case Some(r) =>
        println(s"ID: ${r.id}")
        println(s"Сервис: ${r.service}")
        println(s"Логин: ${r.username}")
        println(s"Email: ${if(r.email.isEmpty) "-" else r.email}")
        println(s"URL: ${if(r.url.isEmpty) "-" else r.url}")
        println(s"Пароль: ********")
        println(s"Заметка: ${if(r.note.isEmpty) "-" else r.note}")
        println(s"Создан: ${LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(r.createdAt), java.time.ZoneId.systemDefault())}")
        println(s"Изменен: ${LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(r.updatedAt), java.time.ZoneId.systemDefault())}")
      case None => println("Запись не найдена.")
    }
  }

  def showPassword(id: Int): Unit = {
    records.find(_.id == id) match {
      case Some(r) =>
        println(s"Пароль: ${r.password}")
        log("INFO", s"Показан пароль для id=$id")
      case None => println("Запись не найдена.")
    }
  }

  def addRecord(scanner: Scanner): Unit = {
    print("Сервис: "); val svc = scanner.nextLine()
    print("Логин: "); val usr = scanner.nextLine()
    print("Пароль: "); val pwd = readSecret(scanner)
    print("Email (Enter пусто): "); val em = scanner.nextLine()
    print("URL (Enter пусто): "); val url = scanner.nextLine()
    print("Заметка (Enter пусто): "); val nt = scanner.nextLine()

    if (svc.isEmpty || usr.isEmpty || pwd.isEmpty) {
      println("Обязательные поля не заполнены.")
      return
    }

    val now = System.currentTimeMillis()
    val newRec = VaultRecord(nextId, svc, usr, pwd, em, url, nt, now, now)
    records ::= newRec
    nextId += 1
    saveVault()
    log("INFO", s"Добавлена запись id=${newRec.id}")
    println(s"Запись добавлена (ID: ${newRec.id})")
  }

  def editRecord(id: Int, scanner: Scanner): Unit = {
    records.find(_.id == id) match {
      case None => println("Запись не найдена.")
      case Some(oldR) =>
        println("Текущие данные:")
        showRecord(id)

        print("Новый Сервис (Enter оставить): "); val svc = optVal(scanner, oldR.service)
        print("Новый Логин (Enter оставить): "); val usr = optVal(scanner, oldR.username)

        var pwd = oldR.password
        print("Изменить пароль? [y/n]: ")
        if (scanner.nextLine().equalsIgnoreCase("y")) {
          print("Новый Пароль: "); pwd = readSecret(scanner)
        }

        print("Новый Email (Enter оставить): "); val em = optVal(scanner, oldR.email)
        print("Новый URL (Enter оставить): "); val url = optVal(scanner, oldR.url)
        print("Новая Заметка (Enter оставить): "); val nt = optVal(scanner, oldR.note)

        val updatedRec = oldR.copy(
          service = svc, username = usr, password = pwd,
          email = em, url = url, note = nt,
          updatedAt = System.currentTimeMillis()
        )

        records = records.map(r => if (r.id == id) updatedRec else r)
        saveVault()
        log("INFO", s"Изменена запись id=$id")
        println("Запись обновлена.")
    }
  }

  def removeRecord(id: Int, scanner: Scanner): Unit = {
    records.find(_.id == id) match {
      case None => println("Запись не найдена.")
      case Some(r) =>
        if (config.getOrElse("confirmBeforeDelete", "true").toBoolean) {
          println(s"Удалить запись ID:$id (${r.service})? [y/n]: ")
          if (!scanner.nextLine().equalsIgnoreCase("y")) {
            println("Отменено.")
            return
          }
        }
        records = records.filterNot(_.id == id)
        saveVault()
        log("INFO", s"Удалена запись id=$id")
        println("Запись удалена.")
    }
  }

  def searchRecords(query: String): Unit = {
    if (query.trim.isEmpty) {
      println("Введите поисковый запрос.")
      return
    }
    val q = query.toLowerCase
    val found = records.filter { r =>
      r.service.toLowerCase.contains(q) ||
        r.username.toLowerCase.contains(q) ||
        r.url.toLowerCase.contains(q)
    }

    if (found.isEmpty) {
      println("Ничего не найдено.")
    } else {
      println(s"Найдено записей: ${found.size}")
      println(f"${"ID"}%-5s ${"Сервис"}%-20s ${"Логин"}%-20s")
      found.foreach { r =>
        println(f"${r.id}%-5d ${r.service.take(20)}%-20s ${r.username.take(20)}%-20s")
      }
    }
    log("INFO", s"Поиск '$q': найдено ${found.size}")
  }

  def readSecret(scanner: Scanner): String = {
    scanner.nextLine()
  }

  def optVal(scanner: Scanner, current: String): String = {
    val input = scanner.nextLine()
    if (input.isEmpty) current else input
  }
}
