import tkinter as tk
import psycopg2 as pg
import psycopg2.sql as sql
from tkinter import ttk
from tkinter import messagebox

class DemoApp:
    def __init__(self, root):
        self.root = root
        self.root.title("База данных автобусов г.Зеленоград")
        self.root.geometry("700x500")
        self.notebook = ttk.Notebook(root)  # вкладки
        self.notebook.pack(expand=True, fill='both', padx=5, pady=5)

        host,port='localhost','5432'
        db='bus_base'
        user,passwd='postgres','postgres'
        try:
            self.conn=pg.connect(host=host,port=port,dbname=db,user=user,password=passwd)
            self.cur=self.conn.cursor()
        except Exception as e:
            messagebox.showwarning("Внимание", f"{e}")
        
        self.status_var = tk.StringVar()
        self.status_var.set("")
        self.status_bar = ttk.Label(root, textvariable=self.status_var, relief=tk.RIDGE, anchor=tk.W)  # Статус бар
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        self.dict_insert={}
        self.dict_update={}
        self.cols=[]
        self.cols_sel=[]
        self.cols_upd=[]
        self.cols_del=[]
        self.create_tab1()
        self.create_tab2()
        self.create_tab3()
        self.create_tab4()

        ttk.Button(root,text='Зафиксировать',command=self.commit).pack(padx=5,pady=5,side=tk.RIGHT)
        ttk.Button(root,text='Откатить',command=self.rollback).pack(padx=5,pady=5,side=tk.LEFT)

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def create_tab1(self):
        tab_create = ttk.Frame(self.notebook)
        self.notebook.add(tab_create, text="Вставить строки")

        # Группа элементов
        frame = ttk.LabelFrame(tab_create, text="Добавьте новые данные в базу")
        frame.pack(padx=10, pady=10, fill="both", expand=True)

        fr_table=ttk.Frame(frame)
        fr_table.pack(fill=tk.X)
        fr_cols=ttk.Frame(frame)
        fr_cols.pack(fill=tk.BOTH)
        ttk.Label(fr_table, text="Выберите таблицу:").pack(padx=5,pady=5,side=tk.LEFT)
        try:
            self.cur.execute('SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname=\'public\'')
            self.tables=self.cur.fetchall()
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
        self.btn_ins=ttk.Button(fr_table,text='Выбрать',state=tk.DISABLED,command=lambda: self.create_insert_fields(fr_cols,self.combo_insert.get()))
        self.btn_ins.pack(padx=5,side=tk.RIGHT)
        self.combo_insert = ttk.Combobox(fr_table, values=self.tables)
        self.combo_insert.bind("<<ComboboxSelected>>", lambda e: self.btn_ins.configure(state=tk.ACTIVE))
        self.combo_insert.pack(pady=5,fill=tk.X)

    def create_tab2(self):
        tab_read = ttk.Frame(self.notebook)
        self.notebook.add(tab_read, text="Выбрать строки")

        frame = ttk.LabelFrame(tab_read, text="Посмотрите данные из базы")
        frame.pack(padx=10, pady=10, fill="both", expand=True)

        fr_table=ttk.Frame(frame)
        fr_table.pack(fill=tk.X)
        fr_display=ttk.Frame(frame)
        fr_display.pack(fill=tk.BOTH,expand=1)
        ttk.Label(fr_table, text="Выберите таблицу:").pack(padx=5,pady=5,side=tk.LEFT)
        self.btn_display=ttk.Button(fr_table,text='Показать',state=tk.DISABLED,command=lambda: self.create_display_rows(fr_display,self.combo_select.get()))
        self.btn_display.pack(padx=5,side=tk.RIGHT)
        self.combo_select = ttk.Combobox(fr_table, values=self.tables)
        self.combo_select.bind("<<ComboboxSelected>>", lambda e: self.btn_display.configure(state=tk.ACTIVE))
        self.combo_select.pack(pady=5,fill=tk.X)

    def create_tab3(self):
        tab_update = ttk.Frame(self.notebook)
        self.notebook.add(tab_update, text="Изменить строки")

        self.fr_tab3 = ttk.LabelFrame(tab_update, text="Измените данные в базе")
        self.fr_tab3.pack(padx=10, pady=10, fill="both", expand=True)

        fr_table=ttk.Frame(self.fr_tab3)
        fr_table.pack(fill=tk.BOTH,expand=1)
        ttk.Label(fr_table, text="Сначала выберите строку двойным нажатием\nна вкладке \"Выбрать строки\"").pack(padx=5,pady=5,expand=1)

    def create_tab4(self):
        tab_delete = ttk.Frame(self.notebook)
        self.notebook.add(tab_delete, text="Удалить строки")

        self.fr_tab4 = ttk.LabelFrame(tab_delete, text="Удалите данные в базе")
        self.fr_tab4.pack(padx=10, pady=10, fill="both", expand=True)

        fr_table=ttk.Frame(self.fr_tab4)
        fr_table.pack(fill=tk.BOTH,expand=1)
        ttk.Label(fr_table, text="Сначала выберите строки правой кнопкой мыши\nна вкладке \"Выбрать строки\"").pack(padx=5,pady=5,expand=1)

    # --- Вспомогательные функции ---
    def update_status(self, text):
        self.status_var.set(text)
    
    def convert(self,string):
        return None if (string=='' or string=='None' or string=='null') else string

    def create_insert_fields(self,frame,table):
        for child in frame.winfo_children():
            child.destroy()
        self.cols.clear()
        try:
            self.cur.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name=\'{table}\' ORDER BY ordinal_position;')
            for i in self.cur.fetchall():
                self.cols.append(i[0])
                fr_col=ttk.Frame(frame)
                fr_col.pack(fill=tk.X)
                ttk.Label(fr_col, text=f"{i[0]}:").pack(padx=5,pady=5,side=tk.LEFT)
                ent=ttk.Entry(fr_col)
                ent.pack(fill=tk.X)
                self.dict_insert[i[0]]=ent
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
        ttk.Button(frame,text='Вставить строку',command=lambda: self.insert_into(table)).pack(pady=5)
        self.update_status(f'Выбрана таблица {table}')
    
    def sort_selected(self,treew,table):
        try:
            self.cur.execute(f'SELECT * FROM {table} ORDER BY {self.sort_sel.get()};')
            rows=self.cur.fetchall()
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
        for i in treew.get_children():
            treew.delete(i)
        for row in rows:
            treew.insert('',tk.END,values=row)
    
    def create_display_rows(self,frame,table):
        for child in frame.winfo_children():
            child.destroy()
        self.cols_sel.clear()
        try:
            self.cur.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name=\'{table}\' ORDER BY ordinal_position;')
            for c in self.cur.fetchall():
                self.cols_sel.append(c[0])
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
        fr_table=ttk.Frame(frame)
        fr_table.pack(side=tk.TOP,expand=1,fill=tk.BOTH)
        tree=ttk.Treeview(fr_table,columns=self.cols_sel,show='headings')
        for c in range(len(self.cols_sel)):
            tree.column(f'#{c+1}',stretch=1,width=10)
        self.sort_sel=tk.StringVar()
        fr_sort=ttk.Frame(fr_table)
        fr_sort.pack(anchor=tk.W)
        ttk.Label(fr_sort,text='Сортировать по:').pack(padx=5,side=tk.LEFT)
        rows=[]
        try:
            self.cur.execute(f'SELECT * FROM {table};')
            rows=self.cur.fetchall()
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
        for row in rows:
                tree.insert('',tk.END,values=row)
        for i,c in enumerate(self.cols_sel):
            tree.heading(c,text=c)
            ttk.Radiobutton(fr_sort,text=c,variable=self.sort_sel,value=c,command=lambda:self.sort_selected(tree,table)).pack(padx=10,side=tk.LEFT)
        tree.pack(fill=tk.BOTH,expand=1,side=tk.LEFT)
        tree.bind('<Double-ButtonPress-1>',lambda e: self.create_update_fields(self.fr_tab3,table,[tree.item(it)["values"] for it in tree.selection()]))
        tree.bind('<ButtonPress-3>',lambda e: self.create_delete_field(self.fr_tab4,table,[tree.item(it)["values"] for it in tree.selection()]))
        
        scroll=ttk.Scrollbar(fr_table,orient=tk.VERTICAL,command=tree.yview)
        scroll.pack(side=tk.RIGHT,fill=tk.Y)
        tree.configure(yscroll=scroll.set)
        ttk.Label(frame,text=f'Выведено строк: {self.cur.rowcount}').pack(side=tk.BOTTOM)
        self.update_status(f'Показана таблица {table}')

    def insert_into(self,table):
        sql_insert=sql.SQL(f'INSERT INTO {table} ({",".join(self.cols)}) VALUES ({",".join([r"%s" for i in self.cols])});')
        try:
            self.cur.execute(sql_insert,[self.convert(self.dict_insert[i].get()) for i in self.cols])
            messagebox.showinfo("Статус", f"{self.cur.statusmessage},\nЗатронуто строк: {self.cur.rowcount}")
            self.update_status(f"Строка вставлена")
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
            self.update_status('Ошибка вставки, база восстановлена из последней фиксации')
    
    def create_update_fields(self,frame,table,sel_values):
        if len(sel_values)==1:
            sel_values=sel_values[0]
            for child in frame.winfo_children():
                child.destroy()
            self.cols_upd.clear()
            try:
                self.cur.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name=\'{table}\' ORDER BY ordinal_position;')
                for i in self.cur.fetchall():
                    self.cols_upd.append(i[0])
            except Exception as e:
                messagebox.showwarning("Внимание", f"{e}")
            for ind,c in enumerate(self.cols_upd):
                fr_col=ttk.Frame(frame)
                fr_col.pack(fill=tk.X)
                ttk.Label(fr_col, text=f"{c}:").pack(padx=5,pady=5,side=tk.LEFT)
                ent=ttk.Entry(fr_col)
                ent.pack(fill=tk.X)
                self.dict_update[c]=ent
                ent.insert(0,sel_values[ind])
            self.update_status(f'Выбрана строка {", ".join([str(i) for i in sel_values])}')
            self.notebook.select(2)
            ttk.Button(frame,text='Обновить строку',command=lambda: self.update(table,sel_values)).pack(pady=5)
    
    def update(self,table,sel_val):
        sql_row=[rf"{e}=%s" if sel_val[i]!='None' else f"{e} IS %s" for i,e in enumerate(self.cols_upd)]
        sql_update=sql.SQL(f'UPDATE {table} SET {",".join([rf"{i}=%s" for i in self.cols_upd])} WHERE {" AND ".join(sql_row)};')
        args=[i for j in [[self.convert(self.dict_update[i].get()) for i in self.cols_upd],[self.convert(str(v)) for v in sel_val]] for i in j]
        try:
            self.cur.execute(sql_update,args)
            messagebox.showinfo("Статус", f"{self.cur.statusmessage},\nИзменено строк: {self.cur.rowcount}")
            self.update_status(f"Строка изменена")
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
            self.update_status(f"Ошибка изменения, база восстановлена из последней фиксации")
    
    def create_delete_field(self,frame,table,sel_values):
        if len(sel_values)>0:
            for child in frame.winfo_children():
                child.destroy()
            self.cols_del.clear()
            try:
                self.cur.execute(f'SELECT column_name FROM information_schema.columns WHERE table_name=\'{table}\' ORDER BY ordinal_position;')
                for i in self.cur.fetchall():
                    self.cols_del.append(i[0])
            except Exception as e:
                messagebox.showwarning("Внимание", f"{e}")
            ttk.Label(frame,text='Удалить строки?').pack(padx=5)
            tree=ttk.Treeview(frame,columns=self.cols_del,show='headings')
            for i in range(len(self.cols_del)):
                tree.column(f'#{i+1}',stretch=1,width=10)
            for i in self.cols_del:
                tree.heading(i,text=i)
            tree.pack(pady=5,fill=tk.BOTH,expand=1)
            for row in sel_values:
                tree.insert('',tk.END,values=row)
            self.notebook.select(3)
            ttk.Button(frame,text='Удалить',command=lambda: self.delete_from(table,sel_values)).pack(pady=5,side=tk.BOTTOM)
            self.update_status('Выбраны строки для удаления')

    def delete_from(self,table,sel_vals):
        sql_row=lambda s_v: " AND ".join([rf"{e}=%s" if s_v[i]!='None' else f"{e} IS %s" for i,e in enumerate(self.cols_del)])
        row=lambda s_v: f'({sql_row(s_v)})'
        sql_delete=sql.SQL(f'DELETE FROM {table} WHERE {" OR ".join([row(i) for i in sel_vals])};')
        args=[self.convert(str(i)) for v in sel_vals for i in v]
        try:
            self.cur.execute(sql_delete,args)
            messagebox.showinfo("Статус", f"{self.cur.statusmessage},\nУдалено строк: {self.cur.rowcount}")
            self.update_status(f"Строки удалены")
        except Exception as e:
            self.conn.rollback()
            messagebox.showwarning("Внимание", f"{e}")
            self.update_status(f"Ошибка удаления, база восстановлена из последней фиксации")

    def commit(self):
        try:
            self.conn.commit()
            self.update_status(f'Изменения внесены')
        except Exception as e:
            messagebox.showwarning("Внимание", f"{e}")
    
    def rollback(self):
        try:
            self.conn.rollback()
            self.update_status(f'Изменения отменены')
        except Exception as e:
            messagebox.showwarning("Внимание", f"{e}")

if __name__ == "__main__":
    root = tk.Tk()
    
    style = ttk.Style()
    style.theme_use('clam')
    
    app = DemoApp(root)
    root.mainloop()
